package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/capacity"
	"github.com/liran/sink/internal/protocol"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage/memory"
)

type expansionPublisher struct {
	pool        *capacity.Pool
	sourceBytes int
	used        int64
	unique      map[*byte]bool
}

func (p *expansionPublisher) Publish(_ context.Context, req queue.PublishRequest) (queue.PublishResponse, error) {
	p.used = p.pool.Used()
	response := queue.PublishResponse{Results: make([]queue.PublishResult, len(req.Mutations))}
	for i, mutation := range req.Mutations {
		source := mutation.Write.GetMerge().GetLuaProgram().GetSource()
		p.sourceBytes += len(source)
		p.unique[&source[0]] = true
		response.Results[i].Status = queue.PublishStatusAccepted
	}
	return response, nil
}

func TestSharedLuaExpansionStaysInsideMemoryCapacity(t *testing.T) {
	for _, maximum := range []int64{2 << 20, 32 << 20} {
		t.Run(fmt.Sprint(maximum), func(t *testing.T) {
			options := capacity.Options{Bytes: maximum, BurstPercent: 10, WaitTimeout: 20 * time.Millisecond}
			pool, err := capacity.New(options)
			if err != nil {
				t.Fatal(err)
			}
			server := completionServer(t, memory.New()).server
			server.memory = pool
			publisher := &expansionPublisher{pool: pool, unique: make(map[*byte]bool)}
			server.publisher = publisher
			source := []byte("--" + strings.Repeat("x", 60000) + "\nreturn function(current, incoming) return incoming end")
			digest := sha256.Sum256(source)
			declaration := &sink.LuaProgram{Source: source, Sha256: digest[:]}
			request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED, LuaPrograms: []*sink.LuaProgram{declaration}}
			for i := range 256 {
				operation := completionMerge(fmt.Sprint("expanded-", i), 1)
				reference := &sink.LuaProgram{Sha256: digest[:]}
				operation.GetMerge().LuaProgram = reference
				request.Operations = append(request.Operations, operation)
			}
			scope := pool.NewScope()
			defer scope.Release()
			ctx := capacity.WithScope(t.Context(), scope)
			handler := func(ctx context.Context, req any) (any, error) { return server.Write(ctx, req.(*sink.WriteRequest)) }
			response, err := protocol.MemoryInterceptor(ctx, request, nil, handler)
			if err != nil {
				t.Fatal(err)
			}
			result := response.(*protocol.ManagedMessage).Message.(*sink.WriteResponse)
			for _, op := range result.Results {
				if maximum == 2<<20 {
					if op.GetStatus() != sink.WriteStatus_WRITE_STATUS_FAILED || op.GetFailure().GetCode() != sink.FailureCode_FAILURE_CODE_RESOURCE_EXHAUSTED || !op.GetFailure().GetRetryable() {
						t.Fatalf("expanded source was not rejected before enqueue: %v", op)
					}
				} else if op.GetStatus() != sink.WriteStatus_WRITE_STATUS_ACCEPTED {
					t.Fatalf("sufficient capacity rejected the request: %v", op)
				}
			}
			if maximum == 2<<20 && publisher.sourceBytes != 0 {
				t.Fatal("expanded mutation reached the publisher before reserving capacity")
			}
			if maximum == 32<<20 {
				if len(publisher.unique) != 1 {
					t.Fatalf("shared declaration became %d independent copies", len(publisher.unique))
				}
				if publisher.used < int64(publisher.sourceBytes) || publisher.used > maximum {
					t.Fatalf("encoded payload not covered: used=%d payload=%d", publisher.used, publisher.sourceBytes)
				}
			}
			scope.Release()
			if pool.Used() != 0 {
				t.Fatal("completed publish leaked memory capacity")
			}
		})
	}
}
