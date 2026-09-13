//go:build integration

package mongodb_test

import (
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeMongoQueryPreservesBusinessFieldsAndLiteralStageNames(t *testing.T) {
	fixture := newIntegrationFixture(t)
	seed := bson.D{{Key: "_id", Value: "original"}, {Key: "tailable", Value: false}, {Key: "singleBatch", Value: true}}
	if _, err := fixture.collection.InsertOne(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "tailable", Value: false}, {Key: "singleBatch", Value: true}}
	find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter}}
	match := bson.D{{Key: "$match", Value: filter}}
	literal := bson.D{{Key: "$literal", Value: bson.D{{Key: "$merge", Value: "data"}, {Key: "$out", Value: "data"}, {Key: "$changeStream", Value: "data"}}}}
	project := bson.D{{Key: "$project", Value: bson.D{{Key: "value", Value: literal}}}}
	aggregate := bson.D{{Key: "aggregate", Value: "documents"}, {Key: "pipeline", Value: bson.A{match, project}}}
	for _, command := range []bson.D{find, aggregate} {
		request := mongoNativeRequest(t, fixture.database, command)
		query := storage.QueryRequest{Request: request, PageSize: 2}
		page, err := fixture.store.Query(t.Context(), query)
		if err != nil || len(page.Documents) != 1 || page.HasMore {
			t.Fatalf("business fields rejected: command=%s page=%+v error=%v", command, page, err)
		}
		if command[0].Key == "aggregate" && bson.Raw(page.Documents[0].Payload).Lookup("value", "$merge").StringValue() != "data" {
			t.Fatal("literal pipeline-stage name was not preserved")
		}
		countRequest := storage.CountRequest{Request: request}
		count, err := fixture.store.Count(t.Context(), countRequest)
		if err != nil || count.Count != 1 || count.Estimated {
			t.Fatalf("business fields rejected by count: count=%+v error=%v", count, err)
		}
	}
	request := mongoNativeRequest(t, fixture.database, find)
	scan := storage.ScanRequest{Request: request, BatchSize: 2}
	page, err := fixture.store.Scan(t.Context(), scan)
	if err != nil || len(page.Documents) != 1 || len(page.NextCursor) != 0 {
		t.Fatalf("business fields rejected by scan: page=%+v error=%v", page, err)
	}
}
