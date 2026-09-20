package mongodb

import (
	"errors"
	"testing"

	"github.com/batchstream/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestDuplicateConstraintsRetainCauseWithoutRetrying(t *testing.T) {
	for _, code := range []int{11000, 11001, 12582, 16460} {
		failure := mongo.WriteError{Code: code, Message: "write failed: E11000 duplicate key index: email_1 dup key: { email: already-used }"}
		err := classifyWriteError(failure)
		kind, retryable := storage.ErrorDetails(err)
		var original mongo.WriteError
		if kind != storage.ErrorCodePreconditionFailed || retryable || !errors.As(err, &original) || original.Message != failure.Message {
			t.Fatalf("constraint lost its cause or became retryable: %v", err)
		}
		concern := &mongo.WriteConcernError{Code: 64, Message: "acknowledgement unknown"}
		exception := mongo.WriteException{WriteErrors: []mongo.WriteError{failure}, WriteConcernError: concern}
		kind, retryable = storage.ErrorDetails(classifyOperationError(exception))
		if kind != storage.ErrorCodeUnavailable || !retryable {
			t.Fatal("constraint result hid write concern uncertainty")
		}
	}
	unknown := mongo.WriteError{Code: 16460, Message: "unrelated error"}
	if kind, _ := storage.ErrorDetails(classifyWriteError(unknown)); kind == storage.ErrorCodePreconditionFailed {
		t.Fatal("non-duplicate legacy error was classified as a constraint")
	}
}

func TestIDDuplicateRequiresPositiveIndexIdentity(t *testing.T) {
	cases := []struct {
		pattern bson.D
		message string
		want    bool
	}{
		{pattern: bson.D{{Key: "_id", Value: 1}}, want: true},
		{pattern: bson.D{{Key: "email", Value: 1}}, message: " index: _id_ dup key:", want: false},
		{pattern: bson.D{{Key: "_id", Value: 1}, {Key: "email", Value: 1}}, want: false},
		{message: "E11000 duplicate key error collection: db.docs index: _id_ dup key: { _id: a }", want: true},
		{message: "E11000 duplicate key error collection: db.docs index: email_1 dup key: { email: a }"},
		{message: "E11000 duplicate key error"},
	}
	for _, test := range cases {
		failure := mongo.WriteError{Code: 11000, Message: test.message}
		if test.pattern != nil {
			document := bson.D{{Key: "keyPattern", Value: test.pattern}}
			raw, err := bson.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			failure.Raw = raw
		}
		if got := isIDDuplicate(failure); got != test.want {
			t.Fatalf("pattern=%v message=%q: got %t, want %t", test.pattern, test.message, got, test.want)
		}
	}
}
