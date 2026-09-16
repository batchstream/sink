// Package forwarding contains the private Gateway-to-Engine resource contract.
package forwarding

import (
	"context"
	"sync"

	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/storage"
)

const Version = 4

// EnvelopeBytes is reserved in the private transport in addition to the public message cap.
const EnvelopeBytes = 4096

type Kind int

const (
	Snapshots Kind = iota
	Inputs
	Outputs
	Returns
	kinds
)

type Tracker struct {
	mu     sync.Mutex
	limits [kinds]int
	used   [kinds]int
}

type contextKey int

const trackerKey contextKey = 1

func NewTracker(grant *forward.Budget, maximum int) *Tracker {
	t := &Tracker{}
	raw := [...]uint64{grant.GetSnapshots(), grant.GetInputs(), grant.GetOutputs(), grant.GetReturns()}
	for i, size := range raw {
		t.limits[i] = int(min(size, uint64(maximum)))
	}
	return t
}

func WithTracker(ctx context.Context, tracker *Tracker) context.Context {
	return context.WithValue(ctx, trackerKey, tracker)
}
func FromContext(ctx context.Context) *Tracker {
	tracker, _ := ctx.Value(trackerKey).(*Tracker)
	return tracker
}
func (t *Tracker) Limit(kind Kind, maximum int) int {
	if t == nil {
		return maximum
	}
	return min(maximum, t.limits[kind])
}
func (t *Tracker) Observe(kind Kind, used int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.used[kind] = max(t.used[kind], used)
	t.mu.Unlock()
}
func (t *Tracker) Fresh(kind Kind, maximum int) *storage.ReadBudget {
	if t == nil {
		return storage.NewReadBudget(maximum)
	}
	limit := t.Limit(kind, maximum)
	return storage.NewTrackedReadBudget(limit, func(used int) { t.Observe(kind, used) })
}
func (t *Tracker) Usage() *forward.Budget {
	t.mu.Lock()
	defer t.mu.Unlock()
	usage := &forward.Budget{Snapshots: uint64(t.used[Snapshots]), Inputs: uint64(t.used[Inputs]), Outputs: uint64(t.used[Outputs]), Returns: uint64(t.used[Returns])}
	return usage
}
func FullBudget(maximum int) *forward.Budget {
	size := uint64(maximum)
	budget := &forward.Budget{Snapshots: size, Inputs: size, Outputs: size, Returns: size}
	return budget
}
