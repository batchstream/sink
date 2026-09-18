package mongodb

import (
	"context"

	"github.com/liran/sink/internal/capacity"
)

// The driver allocates a complete wire message before exposing cursor.Current.
// This opaque allowance exists only while reading from MongoDB; document copies
// are acquired separately at their actual sizes. It is not an RPC admission fee.
func acquireWireMemory(ctx context.Context) (*capacity.Lease, error) {
	lease := capacity.FromContext(ctx).NewLease()
	lease.MarkOpaque()
	if err := capacity.Grow(ctx, lease, 48<<20); err != nil {
		capacity.Close(lease)
		return nil, err
	}
	return lease, nil
}
