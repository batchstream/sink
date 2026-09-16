package gateway

import (
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc/resolver"
)

func replicaView(t *testing.T, gateway *Server, engines []fixtureEngine) *discovery {
	t.Helper()
	route, err := routeFor(gateway.current, engines[0].store)
	if err != nil {
		t.Fatal(err)
	}
	d := &discovery{ready: make(chan struct{}), built: make(chan struct{}), maximum: gateway.pool.maximum, idleSince: time.Now()}
	close(d.built)
	state := resolver.State{}
	for _, engine := range engines {
		address := resolver.Address{Addr: engine.target}
		state.Addresses = append(state.Addresses, address)
	}
	if err := d.UpdateState(state); err != nil {
		t.Fatal(err)
	}
	gateway.pool.mu.Lock()
	if gateway.pool.discoveries == nil {
		gateway.pool.discoveries = make(map[Route]*discovery)
	}
	gateway.pool.discoveries[route] = d
	gateway.pool.mu.Unlock()
	return d
}

func TestRecordAffinityAcrossGatewaysAndRPCBoundaries(t *testing.T) {
	engines := []fixtureEngine{testEngine(t, "a", 16384), testEngine(t, "a", 16384), testEngine(t, "a", 16384)}
	first := testGateway(t, 16384, engines[0])
	second := testGateway(t, 16384, engines[0])
	replicaView(t, first, engines)
	reversed := slices.Clone(engines)
	slices.Reverse(reversed)
	replicaView(t, second, reversed)
	// Interleaved repeated records exercise grouping and public result indexes.
	write := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
	for i := range 60 {
		write.Operations = append(write.Operations, put("a", fmt.Sprintf("key-%d", i%30), false))
	}
	written, err := first.Write(t.Context(), write)
	if err != nil {
		t.Fatal(err)
	}
	read := &sink.ReadRequest{}
	for i, result := range written.Results {
		if int(result.OperationIndex) != i || result.Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatal(written)
		}
		operation := &sink.ReadOperation{Address: write.Operations[i].Address}
		read.Operations = append(read.Operations, operation)
	}
	found, err := second.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range found.Results {
		if int(result.OperationIndex) != i || result.Status != sink.ReadStatus_READ_STATUS_FOUND {
			t.Fatalf("cross-Gateway affinity lost: %v", found)
		}
	}
	counts := make([]int, len(engines))
	for _, operation := range read.Operations[:30] {
		owners := 0
		request := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
		for index, engine := range engines {
			response, err := engine.core.Read(t.Context(), request)
			if err != nil {
				t.Fatal(err)
			}
			if response.Results[0].Status == sink.ReadStatus_READ_STATUS_FOUND {
				owners++
				counts[index]++
			}
		}
		if owners != 1 {
			t.Fatalf("record has %d owners", owners)
		}
	}
	for _, count := range counts {
		if count == 0 {
			t.Fatalf("replica received no records: %v", counts)
		}
	}
	deletion := &sink.DeleteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
	for _, operation := range read.Operations[:30] {
		item := &sink.DeleteOperation{Address: operation.Address}
		deletion.Operations = append(deletion.Operations, item)
	}
	deleted, err := second.Delete(t.Context(), deletion)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range deleted.Results {
		if result.Status != sink.DeleteStatus_DELETE_STATUS_APPLIED {
			t.Fatal(deleted)
		}
	}
	found, err = first.Read(t.Context(), read)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range found.Results {
		if result.Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
			t.Fatal(found)
		}
	}
}

func TestAffinityMembershipChangesMoveOnlyAffectedOwners(t *testing.T) {
	original := []Route{{endpoint: "127.0.0.1:9000"}, {endpoint: "[::1]:9000"}, {endpoint: "127.0.0.2:9000"}}
	newEndpoint := Route{endpoint: "127.0.0.3:9000"}
	added := append(slices.Clone(original), newEndpoint)
	removed := original[:2]
	moved := 0
	for i := range 1000 {
		identity := fmt.Sprintf("sink://a/anything/opaque/path/s:%d", i)
		owner := affinityRoute(identity, original)
		afterAdd := affinityRoute(identity, added)
		if afterAdd != owner {
			moved++
			if afterAdd != added[3] {
				t.Fatal("adding one endpoint moved an unrelated owner")
			}
		}
		afterRemove := affinityRoute(identity, removed)
		if owner != original[2] && afterRemove != owner {
			t.Fatal("removal moved an unrelated owner")
		}
	}
	if moved < 150 || moved > 350 {
		t.Fatalf("unexpected scale-out movement: %d/1000", moved)
	}
}

func TestReplicaReturnBudgetRemainsScopedToOriginalRPC(t *testing.T) {
	engines := []fixtureEngine{testEngine(t, "a", 200), testEngine(t, "a", 200)}
	gateway := testGateway(t, 200, engines[0])
	replicaView(t, gateway, engines)
	routes, release, err := gateway.pool.destinations(t.Context(), gateway.current.routes["a"])
	if err != nil {
		t.Fatal(err)
	}
	release()
	first := put("a", "first", true)
	firstOwner := affinityRoute(first.Address.GetUri(), routes)
	var second *sink.WriteOperation
	for i := 0; i < 1000; i++ {
		candidate := put("a", fmt.Sprintf("second-%d", i), true)
		if affinityRoute(candidate.Address.GetUri(), routes) != firstOwner {
			second = candidate
			break
		}
	}
	if second == nil {
		t.Fatal("could not find another owner")
	}
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{first, second}}
	response, err := gateway.Write(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || response.Results[1].GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED {
		t.Fatal(response)
	}
	operation := &sink.ReadOperation{Address: second.Address}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{operation}}
	result, err := gateway.Read(t.Context(), read)
	if err != nil || result.Results[0].Status != sink.ReadStatus_READ_STATUS_NOT_FOUND {
		t.Fatalf("over-budget write committed: %v %v", result, err)
	}
}

func TestDiscoverySnapshotsSurviveRefreshAndErrors(t *testing.T) {
	d := &discovery{ready: make(chan struct{}), maximum: 2}
	state := resolver.State{Addresses: []resolver.Address{{Addr: "127.0.0.1:80"}, {Addr: "[::1]:80"}, {Addr: "127.0.0.1:80"}}}
	if err := d.UpdateState(state); err != nil {
		t.Fatal(err)
	}
	before, err := d.snapshot(t.Context())
	if err != nil || len(before) != 2 {
		t.Fatalf("%v %v", before, err)
	}
	d.ReportError(errors.New("DNS unavailable"))
	retained, err := d.snapshot(t.Context())
	if err != nil || !slices.Equal(before, retained) {
		t.Fatal("DNS error discarded last successful view")
	}
	state.Addresses = state.Addresses[:1]
	if err := d.UpdateState(state); err != nil {
		t.Fatal(err)
	}
	current, err := d.snapshot(t.Context())
	if err != nil || len(current) != 1 || len(before) != 2 {
		t.Fatal("refresh mutated an existing request snapshot")
	}
	empty := resolver.State{}
	if err := d.UpdateState(empty); err != nil {
		t.Fatal(err)
	}
	if _, err := d.snapshot(t.Context()); err == nil {
		t.Fatal("empty membership selected a removed endpoint")
	}
}

func TestGatewayLeavesCustomStorePathOpaque(t *testing.T) {
	backend := testEngine(t, "a", 4096)
	gateway := testGateway(t, 4096, backend)
	operation := put("a", "placeholder", false)
	operation.Address.Uri = "sink://a/tenant/bucket/object/version"
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{operation}}
	written, err := gateway.Write(t.Context(), request)
	if err != nil || written.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
		t.Fatalf("opaque path rejected: %v %v", written, err)
	}
	readOperation := &sink.ReadOperation{Address: operation.Address}
	read := &sink.ReadRequest{Operations: []*sink.ReadOperation{readOperation}}
	found, err := gateway.Read(t.Context(), read)
	if err != nil || found.Results[0].Status != sink.ReadStatus_READ_STATUS_FOUND {
		t.Fatalf("opaque path lost: %v %v", found, err)
	}
}

func TestPassthroughCannotHideReplicaMembership(t *testing.T) {
	for _, target := range []string{"passthrough:///engines:8080", "passthrough://ignored/127.0.0.1:8080", "passthrough:///127.0.0.1"} {
		if _, err := resolveTarget(target); err == nil {
			t.Fatalf("accepted opaque membership: %s", target)
		}
	}
	for _, target := range []string{"engines:8080", "dns:///engines:8080", "passthrough:///127.0.0.1:8080", "passthrough:///[::1]:8080"} {
		if _, err := resolveTarget(target); err != nil {
			t.Fatalf("rejected supported target %s: %v", target, err)
		}
	}
}
