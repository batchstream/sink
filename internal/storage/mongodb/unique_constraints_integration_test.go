//go:build integration

package mongodb_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/service"
	"github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/worker"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type constraintStorage struct {
	storage.Storage
	writes int
}

func (s *constraintStorage) Write(ctx context.Context, request storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes++
	return s.Storage.Write(ctx, request)
}

func constraintServer(t *testing.T, fixture *integrationFixture) (*service.Server, *constraintStorage) {
	t.Helper()
	luaOptions := merge.LuaOptions{}
	engine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		t.Fatal(err)
	}
	observed := &constraintStorage{Storage: fixture.store}
	opts := service.Options{BoundStore: "primary", Storage: observed, Lua: engine}
	server, err := service.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return server, observed
}

type constraintOperationOptions struct {
	database string
	id       string
	action   string
	email    string
}

func constraintOperation(t *testing.T, opts constraintOperationOptions) *sink.WriteOperation {
	t.Helper()
	value := bson.D{{Key: "email", Value: opts.email}}
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	document := &sink.Document{Encoding: sink.DocumentEncoding_DOCUMENT_ENCODING_BSON, Payload: payload}
	kind := &sink.RecordKey_StringValue{StringValue: opts.id}
	key := &sink.RecordKey{Kind: kind}
	address := &sink.RecordAddress{Store: "primary", Namespace: opts.database, Dataset: "documents", Key: key}
	operation := &sink.WriteOperation{Address: address}
	if opts.action == "merge" {
		program := &sink.LuaProgram{Source: []byte(`return function(current, incoming) return incoming end`)}
		mutation := &sink.MergeOperation{IncomingDocument: document, LuaProgram: program}
		action := &sink.WriteOperation_Merge{Merge: mutation}
		operation.Action = action
	} else {
		mode := sink.WriteMode_WRITE_MODE_UPSERT
		if opts.action == "create" {
			mode = sink.WriteMode_WRITE_MODE_CREATE
		} else if opts.action == "replace" {
			mode = sink.WriteMode_WRITE_MODE_REPLACE
		}
		put := &sink.PutOperation{Mode: mode, Document: document}
		action := &sink.WriteOperation_Put{Put: put}
		operation.Action = action
	}
	return operation
}

func TestMongoDBUniqueConstraintsPreservePublicWriteStatus(t *testing.T) {
	for _, action := range []string{"create", "upsert", "replace", "merge"} {
		for _, count := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/%d", action, count), func(t *testing.T) {
				fixture := newIntegrationFixture(t)
				indexOptions := options.Index().SetUnique(true)
				index := mongo.IndexModel{Keys: bson.D{{Key: "email", Value: 1}}, Options: indexOptions}
				if _, err := fixture.collection.Indexes().CreateOne(t.Context(), index); err != nil {
					t.Fatal(err)
				}
				seed := bson.D{{Key: "_id", Value: "owner"}, {Key: "email", Value: "taken"}}
				if _, err := fixture.collection.InsertOne(t.Context(), seed); err != nil {
					t.Fatal(err)
				}
				request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED}
				for i := range count {
					id := fmt.Sprintf("target-%d", i)
					if action == "replace" || action == "merge" {
						seed := bson.D{{Key: "_id", Value: id}, {Key: "email", Value: id}}
						if _, err := fixture.collection.InsertOne(t.Context(), seed); err != nil {
							t.Fatal(err)
						}
					}
					opts := constraintOperationOptions{database: fixture.database, id: id, action: action, email: "taken"}
					request.Operations = append(request.Operations, constraintOperation(t, opts))
				}
				server, observed := constraintServer(t, fixture)
				response, err := server.Write(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				for i, result := range response.Results {
					failure := result.GetFailure()
					if result.OperationIndex != uint32(i) || result.Status != sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED || failure.GetCode() != sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED || failure.GetRetryable() || !strings.Contains(failure.GetMessage(), "email_1") {
						t.Fatalf("unique constraint changed the public contract: %v", result)
					}
				}
				if observed.writes != 1 {
					t.Fatalf("unique constraint retried through CAS: %d writes", observed.writes)
				}
				filter := bson.D{{Key: "email", Value: "taken"}}
				matches, err := fixture.collection.CountDocuments(t.Context(), filter)
				if err != nil || matches != 1 {
					t.Fatalf("unique owner changed: matches=%d error=%v", matches, err)
				}
			})
		}
	}
}

func TestMongoDBQueuedConstraintFailureAllowsFollowingCorrection(t *testing.T) {
	fixture := newIntegrationFixture(t)
	indexOptions := options.Index().SetUnique(true)
	index := mongo.IndexModel{Keys: bson.D{{Key: "email", Value: 1}}, Options: indexOptions}
	if _, err := fixture.collection.Indexes().CreateOne(t.Context(), index); err != nil {
		t.Fatal(err)
	}
	seed := bson.D{{Key: "_id", Value: "owner"}, {Key: "email", Value: "taken"}}
	if _, err := fixture.collection.InsertOne(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
	server, _ := constraintServer(t, fixture)
	processor, err := worker.NewProcessor(server)
	if err != nil {
		t.Fatal(err)
	}
	opts := constraintOperationOptions{database: fixture.database, id: "target", action: "upsert", email: "taken"}
	invalid := queue.Mutation{Write: constraintOperation(t, opts)}
	opts.email = "available"
	corrected := queue.Mutation{Write: constraintOperation(t, opts)}
	mutations := []queue.Mutation{invalid, corrected}
	results := processor.HandleBatch(t.Context(), mutations)
	var failure *worker.ApplyError
	if len(results) != 2 || !errors.As(results[0], &failure) || failure.Retryable() || results[1] != nil {
		t.Fatalf("constraint blocked queued correction: %v", results)
	}
	filter := bson.D{{Key: "_id", Value: "target"}}
	var stored bson.Raw
	if err := fixture.collection.FindOne(t.Context(), filter).Decode(&stored); err != nil {
		t.Fatal(err)
	}
	if stored.Lookup("email").StringValue() != "available" {
		t.Fatal("following corrected document was not applied")
	}
}
