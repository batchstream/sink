package service

import (
	"context"

	"github.com/liran/sink/internal/storage"
)

// responseGroups maps coalesced operations to their local response boundaries.
// Streams have one group per result; internal batch calls retain caller ownership.
type responseGroups struct {
	owners []int
	count  int
}

func (b *responseGroups) callerCount() int {
	if b == nil {
		return 1
	}
	return b.count
}

func (b *responseGroups) owner(index int) int {
	if b == nil {
		return 0
	}
	return b.owners[index]
}

func (b *responseGroups) fresh(maxBytes int) []*storage.ReadBudget {
	budgets := make([]*storage.ReadBudget, b.callerCount())
	for index := range budgets {
		budgets[index] = storage.NewReadBudget(maxBytes)
	}
	return budgets
}

func (b *responseGroups) add(count int) {
	for range count {
		b.owners = append(b.owners, b.count)
	}
	b.count++
}

func (b *responseGroups) addContext(ctx context.Context, count int) {
	if streaming, _ := ctx.Value(streamingKey{}).(bool); streaming {
		for range count {
			b.add(1)
		}
		return
	}
	b.add(count)
}
func responseGroupsFor(ctx context.Context, count int) *responseGroups {
	if streaming, _ := ctx.Value(streamingKey{}).(bool); !streaming {
		return nil
	}
	budgets := &responseGroups{}
	budgets.addContext(ctx, count)
	return budgets
}
