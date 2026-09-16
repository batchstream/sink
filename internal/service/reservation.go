package service

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// An admitted write can return unused document capacity while waiting for
// storage. Growth is atomic and nonblocking: waiting while retaining documents
// could deadlock several writers that all need their next working set.
type admissionReservation struct {
	pool     *admissionPool
	stores   []string
	bytes    int
	maximum  int
	released bool
}

func (r *admissionReservation) resize(bytes int) error {
	pool := r.pool
	pool.admissionMu.Lock()
	defer pool.admissionMu.Unlock()
	if r.released || bytes < 0 || bytes > r.maximum {
		return errors.New("invalid execution reservation resize")
	}
	delta := bytes - r.bytes
	if delta == 0 {
		return nil
	}
	if delta > 0 {
		full := delta > pool.maxInFlightBytes-pool.inFlightBytes
		for _, waiter := range pool.admissionWaiters {
			if !pool.admissionSlotsFull(*waiter) {
				full = true
				break
			}
		}
		if full {
			store := pool.metrics.RequestStores(r.stores)
			pool.metrics.ObserveAdmissionPoolRejected(store, pool.name, "resize")
			return status.Error(codes.ResourceExhausted, "Sink execution capacity cannot restore the write working set")
		}
	}
	r.bytes = bytes
	pool.inFlightBytes += delta
	pool.adjustStoreBytes(r.stores, delta)
	store := pool.metrics.RequestStores(r.stores)
	pool.metrics.AdjustAdmissionPool(store, pool.name, 0, delta)
	if delta < 0 {
		pool.wakeAdmissionWaiter()
	}
	return nil
}
