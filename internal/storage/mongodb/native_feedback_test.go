package mongodb

import (
	"testing"

	"github.com/batchstream/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestNativeFeedbackDoesNotClassifySemanticCommandErrorsAsOverload(t *testing.T) {
	for _, code := range []int32{2, 9, 13, 26, 59, 72, 11000, 112, 50, 91, 189, 16500} {
		failure := mongo.CommandError{Code: code, Message: "injected native failure"}
		classified := nativeFailure(failure)
		kind, retryable := storage.ErrorDetails(classified)
		wantRetryable := code == 50 || code == 91 || code == 189 || code == 16500
		if retryable != wantRetryable {
			t.Errorf("native command code=%d kind=%v retryable=%t", code, kind, retryable)
		}
		if code == 11000 && kind != storage.ErrorCodePreconditionFailed {
			t.Fatal("duplicate key lost semantic classification")
		}
	}
}

func TestNativeFeedbackFromSuccessfulCommandEnvelope(t *testing.T) {
	for _, field := range []string{"writeErrors", "writeConcernError"} {
		for _, code := range []int32{121, 91} {
			failure := bson.D{{Key: "code", Value: code}, {Key: "errmsg", Value: "injected"}}
			var body any = failure
			if field == "writeErrors" {
				body = bson.A{failure}
			}
			document := bson.D{{Key: "ok", Value: 1}, {Key: field, Value: body}}
			raw, err := bson.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			_, retryable := storage.ErrorDetails(nativeResponseFailure(raw, nil))
			if retryable != (code == 91 || field == "writeConcernError") {
				t.Fatalf("%s %d has incorrect classification", field, code)
			}
		}
	}
}
