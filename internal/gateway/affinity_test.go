package gateway

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc/resolver"
)

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
