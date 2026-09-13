package mongodb

import (
	"errors"
	"strings"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func classifyOperationError(err error) error {
	var failure mongo.WriteException
	if errors.As(err, &failure) && failure.WriteConcernError == nil && len(failure.WriteErrors) == 1 {
		return classifyWriteError(failure.WriteErrors[0])
	}
	return storage.BackendError(err)
}

func classifyWriteError(failure mongo.WriteError) error {
	if mongo.IsDuplicateKeyError(failure) {
		return storage.NewOperationError(storage.ErrorCodePreconditionFailed, false, failure)
	}
	// These per-document replies positively identify a rejected record. Other
	// bulk item errors can be dependency failures, even without a retry label.
	switch failure.Code {
	case 66, 121, 10334: // ImmutableField, DocumentValidationFailure, BSONObjectTooLarge.
		return storage.InvalidArgumentError(failure)
	default:
		return storage.BackendError(failure)
	}
}

// Only a duplicate on the _id index proves a competing insertion of this
// record. Other unique constraints cannot be resolved by rebasing its revision.
func isIDDuplicate(failure mongo.WriteError) bool {
	if !mongo.IsDuplicateKeyError(failure) {
		return false
	}
	for _, raw := range []bson.Raw{failure.Raw, failure.Details} {
		pattern, ok := raw.Lookup("keyPattern").DocumentOK()
		if !ok {
			continue
		}
		fields, err := pattern.Elements()
		return err == nil && len(fields) == 1 && fields[0].Key() == "_id"
	}
	// Older replies may omit keyPattern; require the exact index-name boundary.
	return strings.Contains(failure.Message, " index: _id_ dup key:")
}

func validBulkFailures(failure mongo.BulkWriteException, operations int) bool {
	if len(failure.WriteErrors) == 0 || failure.WriteConcernError != nil {
		return false
	}
	seen := make(map[int]bool, len(failure.WriteErrors))
	for _, item := range failure.WriteErrors {
		if item.Index < 0 || item.Index >= operations || seen[item.Index] {
			return false
		}
		seen[item.Index] = true
	}
	return true
}
