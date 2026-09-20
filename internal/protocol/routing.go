package protocol

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/batchstream/sink-go/uri"

	sink "github.com/batchstream/sink/gen/sink"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CheckStore rejects an entire misrouted request before any side effect.
func CheckStore(request any, store string) error {
	if store == "" {
		return status.Error(codes.FailedPrecondition, "process Store is not configured")
	}
	var stores []string
	switch req := request.(type) {
	case *sink.ReadRequest:
		for _, op := range req.GetOperations() {
			stores = append(stores, RecordStore(op.GetAddress()))
		}
	case *sink.WriteRequest:
		for _, op := range req.GetOperations() {
			stores = append(stores, RecordStore(op.GetAddress()))
		}
	case *sink.DeleteRequest:
		for _, op := range req.GetOperations() {
			stores = append(stores, RecordStore(op.GetAddress()))
		}
	case *sink.ExecuteRequest:
		stores = append(stores, CommandStore(req.GetCommand()))
	case *sink.QueryRequest:
		stores = append(stores, CommandStore(req.GetCommand()))
	case *sink.CountRequest:
		stores = append(stores, CommandStore(req.GetCommand()))
	case *sink.ScanRequest:
		stores = append(stores, CommandStore(req.GetCommand()))
	}
	for _, actual := range stores {
		if actual != store {
			return status.Error(codes.InvalidArgument, "request does not belong to the configured Store")
		}
	}
	return nil
}

// ValidateLuaDeclarations is syntax-independent: the Gateway never compiles Lua.
func ValidateLuaDeclarations(programs []*sink.LuaProgram) error {
	seen := make(map[[sha256.Size]byte][]byte, len(programs))
	for index, program := range programs {
		if program == nil || len(program.GetSource()) == 0 {
			return fmt.Errorf("program %d source is required", index)
		}
		digest := sha256.Sum256(program.GetSource())
		if len(program.GetSha256()) != 0 && !bytes.Equal(program.GetSha256(), digest[:]) {
			return fmt.Errorf("program %d SHA-256 digest does not match source", index)
		}
		if previous, exists := seen[digest]; exists && !bytes.Equal(previous, program.GetSource()) {
			return fmt.Errorf("program %d has a duplicate SHA-256 digest", index)
		}
		seen[digest] = program.GetSource()
	}
	return nil
}

// ParseAddress validates the canonical URI without interpreting Store path rules.
func ParseAddress(address *sink.RecordAddress) (uri.Address, error) {
	parsed, err := uri.Parse(address.GetUri())
	if err == nil && len(parsed.Segments()) == 0 {
		err = fmt.Errorf("record URI requires a resource path")
	}
	return parsed, err
}

func RecordStore(address *sink.RecordAddress) string {
	parsed, err := ParseAddress(address)
	if err != nil {
		return ""
	}
	return parsed.Store()
}

// CommandStore reads only the URI authority. Native path rules belong to adapters.
func CommandStore(command *sink.Command) string {
	address, err := uri.Parse(command.GetUri())
	if err != nil {
		return ""
	}
	return address.Store()
}
