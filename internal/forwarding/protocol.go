// Package forwarding defines the private Gateway-to-Engine transport contract.
package forwarding

import (
	"crypto/sha256"
	"encoding/hex"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const Version = 8

// EnvelopeBytes leaves room for the private wrapper around a public message.
const EnvelopeBytes = 4096

// NotStartedTrailer is set only when Engine proves no backend work was started.
// Its absence leaves undelivered mutation outcomes unknown.
const NotStartedTrailer = "sink-forward-not-started"

// ErrorStatusTrailer fingerprints the exact error returned by the Engine handler.
// It is independent of execution/acceptance evidence in NotStartedTrailer.
const ErrorStatusTrailer = "sink-forward-error"

// ErrorStatusMarker uses the same context-error normalization as the gRPC server.
// A fixed-size digest avoids duplicating potentially large status details.
// An empty marker cannot establish ownership and must not suppress metrics.
func ErrorStatusMarker(err error) string {
	if err == nil {
		return ""
	}
	reply, ok := status.FromError(err)
	if !ok {
		reply = status.FromContextError(err)
	}
	encoded, encodeErr := proto.Marshal(reply.Proto())
	if encodeErr != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
