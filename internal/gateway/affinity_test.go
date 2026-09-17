package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

func TestMembershipWithdrawalRetainsInFlightRequestSnapshot(t *testing.T) {
	for _, stopEarly := range []bool{false, true} {
		t.Run(fmt.Sprintf("stop-before-request-drains=%v", stopEarly), func(t *testing.T) {
			slow := &controlledEngine{store: "a", entered: make(chan struct{}, 1), release: make(chan struct{})}
			old := &controlledEngine{store: "a"}
			first := fixtureEngine{store: "a", target: serveEngine(t, slow)}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			forward.RegisterEngineServer(server, old)
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(server.Stop)
			second := fixtureEngine{store: "a", target: listener.Addr().String()}
			gateway := testGateway(t, 4096, first)
			view := replicaView(t, gateway, []fixtureEngine{first, second})
			routes := []Route{{endpoint: first.target}, {endpoint: second.target}}
			operations := make([]*sink.WriteOperation, 2)
			for i := 0; i < 10000 && (operations[0] == nil || operations[1] == nil); i++ {
				operation := put("a", fmt.Sprintf("snapshot-%d", i), true)
				owner := affinityRoute(operation.Address.Uri, routes)
				index := 0
				if owner.endpoint == second.target {
					index = 1
				}
				operations[index] = operation
			}
			if operations[0] == nil || operations[1] == nil {
				t.Fatal("could not find keys for both owners")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: operations}
			done := make(chan *sink.WriteResponse, 1)
			go func() { response, _ := gateway.Write(ctx, request); done <- response }()
			select {
			case <-slow.entered:
			case <-ctx.Done():
				t.Fatal("first group did not enter")
			}
			state := resolver.State{Addresses: []resolver.Address{{Addr: first.target}}}
			if err := view.UpdateState(state); err != nil {
				t.Fatal(err)
			}
			if stopEarly {
				server.GracefulStop()
			}
			close(slow.release)
			select {
			case response := <-done:
				if response == nil || len(response.Results) != 2 || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
					t.Fatalf("first group failed: %v", response)
				}
				if stopEarly {
					if response.Results[1].GetFailure() == nil || response.Results[1].GetFailure().GetRetryable() || old.calls.Load() != 0 {
						t.Fatalf("late dispatch lost its unknown-outcome contract: %v", response)
					}
				} else if response.Results[1].Status != sink.WriteStatus_WRITE_STATUS_APPLIED || old.calls.Load() != 1 {
					t.Fatalf("withdrawal changed an accepted request's snapshot: %v", response)
				}
			case <-ctx.Done():
				t.Fatal("request did not finish")
			}
		})
	}
}

func TestAffinityRoutingKeepsPublishedHashMapping(t *testing.T) {
	// Fixed vectors protect routing compatibility across mixed-version rollouts,
	// including an address that exceeds the hash buffer's stack capacity.
	routes := []Route{
		{endpoint: "10.0.0.1:8080"},
		{endpoint: "[2001:db8::1]:8080"},
		{endpoint: "engine-" + strings.Repeat("x", 300) + ":8080"},
	}
	for i, expected := range []int{2, 1, 1, 1, 2, 0, 0, 2} {
		identity := fmt.Sprintf("sink://catalog/products/items/s:record-%d", i)
		if got := affinityRoute(identity, routes); got != routes[expected] {
			t.Fatalf("record %d mapped to %q, want %q", i, got.endpoint, routes[expected].endpoint)
		}
	}
	// Preserve the same mapping at and beyond the identity buffer boundary.
	longIdentities := []struct {
		length int
		owner  int
	}{{255, 1}, {256, 1}, {257, 0}, {1024, 2}}
	const prefix = "sink://catalog/products/items/s:"
	for _, vector := range longIdentities {
		identity := prefix + strings.Repeat("x", vector.length-len(prefix))
		if got := affinityRoute(identity, routes); got != routes[vector.owner] {
			t.Fatalf("%d-byte identity mapped to %q, want %q", vector.length, got.endpoint, routes[vector.owner].endpoint)
		}
	}
}

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
