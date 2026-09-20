package service

import (
	"errors"
	"testing"

	sink "github.com/batchstream/sink/gen/sink"
	"github.com/batchstream/sink/internal/storage"
)

func TestFinalWriteConflictsPreservePreconditionStatus(t *testing.T) {
	cases := []struct {
		code      storage.ErrorCode
		retryable bool
		wantCode  sink.FailureCode
	}{
		{code: storage.ErrorCodePreconditionFailed, wantCode: sink.FailureCode_FAILURE_CODE_PRECONDITION_FAILED},
		{code: storage.ErrorCodeConflict, retryable: true, wantCode: sink.FailureCode_FAILURE_CODE_CONFLICT},
	}
	for _, test := range cases {
		cause := errors.New("original constraint or revision conflict")
		failure := storage.NewOperationError(test.code, test.retryable, cause)
		stored := storage.WriteResult{Status: storage.WriteStatusFailed, Err: failure}
		result := &sink.WriteResult{}
		applyWriteResult(result, stored)
		if result.Status != sink.WriteStatus_WRITE_STATUS_PRECONDITION_FAILED || result.GetFailure().GetCode() != test.wantCode || result.Failure.Retryable != test.retryable || result.Failure.Message != cause.Error() {
			t.Fatalf("final conflict contract changed: %v", result)
		}
	}
}
