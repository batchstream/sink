package gateway

import (
	"math"
	"testing"

	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestResponseBudgetComesFromTransportAndSharesOnlyReturns(t *testing.T) {
	request := config.Request{MaxReadBytes: 4096}
	server := &Server{request: request}
	grant, err := server.responseBudget(2)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Returns != 4096-2*1280 {
		t.Fatalf("unreserved response envelope: %v", grant)
	}
	if grant.Snapshots != math.MaxInt64 || grant.Inputs != math.MaxInt64 || grant.Outputs != math.MaxInt64 {
		t.Fatalf("transport ceiling leaked into Store-local execution: %v", grant)
	}
	used := &forward.Budget{Snapshots: 10000, Inputs: 20000, Outputs: 30000, Returns: 100}
	consume(grant, used)
	if grant.Returns != 4096-2*1280-100 || grant.Snapshots != math.MaxInt64 || grant.Outputs != math.MaxInt64 {
		t.Fatalf("settlement charged unrelated Store working sets: %v", grant)
	}
	if _, err := server.responseBudget(4); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("impossible response must fail before forwarding: %v", err)
	}
	native, err := server.responseBudget(0)
	if err != nil || native.GetReturns() != 4096 {
		t.Fatalf("native encoded response: %v %v", native, err)
	}
}
