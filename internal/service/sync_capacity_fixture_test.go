package service

import (
	"github.com/liran/sink/internal/testuri"

	"context"
	"sync/atomic"
	"time"

	"github.com/liran/sink/internal/storage"
)

// Count actual adapter rounds for the same collected RPCs, including admission
// splitting. The optional delay models I/O cost, not production latency.
type syncCapacityStorage struct {
	storage.Storage
	delay  time.Duration
	reads  atomic.Int64
	writes atomic.Int64
}

func (s *syncCapacityStorage) Read(ctx context.Context, request storage.ReadRequest) (storage.ReadResponse, error) {
	s.reads.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Read(ctx, request)
}

func (s *syncCapacityStorage) Write(ctx context.Context, request storage.WriteRequest) (storage.WriteResponse, error) {
	s.writes.Add(1)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	return s.Storage.Write(ctx, request)
}

func (s *syncCapacityStorage) BatchKey(address storage.Address) (string, error) {
	return testuri.BatchKey(address)
}
