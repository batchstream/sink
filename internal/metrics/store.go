package metrics

import (
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/protocol"
)

const (
	multipleStores    = "_multiple"
	unconfiguredStore = "_unconfigured"
)

// storeLabel bounds observations to names captured from configuration at startup.
// Internal callers may also pass a classified cross-store request.
func (m *Metrics) storeLabel(store string) string {
	if store == multipleStores {
		return store
	}
	return m.configuredStore(store)
}

func (m *Metrics) configuredStore(store string) string {
	if m != nil {
		if _, exists := m.stores[store]; exists {
			return store
		}
	}
	return unconfiguredStore
}

// RequestStores attributes a request once, even when it targets several stores.
func (m *Metrics) RequestStores(stores []string) string {
	if len(stores) == 0 {
		return unconfiguredStore
	}
	for _, store := range stores[1:] {
		if store != stores[0] {
			return multipleStores
		}
	}
	return m.configuredStore(stores[0])
}

// RequestStore uses request addresses so failed operations retain attribution.
func (m *Metrics) RequestStore(request any) string {
	switch req := request.(type) {
	case *sink.ReadRequest:
		return requestOperationStore(m, req.GetOperations())
	case *sink.WriteRequest:
		return requestOperationStore(m, req.GetOperations())
	case *sink.DeleteRequest:
		return requestOperationStore(m, req.GetOperations())
	case interface{ GetCommand() *sink.Command }:
		return m.configuredStore(req.GetCommand().GetStore())
	default:
		return unconfiguredStore
	}
}

func requestOperationStore[T interface{ GetAddress() *sink.RecordAddress }](m *Metrics, operations []T) string {
	if len(operations) == 0 {
		return unconfiguredStore
	}
	store := protocol.RecordStore(operations[0].GetAddress())
	for _, operation := range operations[1:] {
		if protocol.RecordStore(operation.GetAddress()) != store {
			return multipleStores
		}
	}
	return m.configuredStore(store)
}

func operationStore[T interface{ GetAddress() *sink.RecordAddress }](m *Metrics, operations []T, index int) string {
	if index >= len(operations) {
		return unconfiguredStore
	}
	return m.configuredStore(protocol.RecordStore(operations[index].GetAddress()))
}
