package search

import (
	"container/list"
	"crypto/sha256"
	"sync"
	"time"
)

const maxScanSizeHints = 256

// Hints affect only fetch size. Seek cursors remain self-contained and work
// unchanged across replicas, restarts and older servers. The command hash
// includes the response byte limit, so a small caller cannot throttle a larger
// budget. Expiration allows later scans to probe for smaller documents again.
type scanSizeCache struct {
	mu      sync.Mutex
	entries map[[sha256.Size]byte]*list.Element
	recent  list.List
}

type scanSizeHint struct {
	key     [sha256.Size]byte
	size    int
	expires time.Time
}

func (c *scanSizeCache) suggest(key [sha256.Size]byte, maximum int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return maximum
	}
	hint := entry.Value.(*scanSizeHint)
	if time.Now().After(hint.expires) {
		delete(c.entries, key)
		c.recent.Remove(entry)
		return maximum
	}
	c.recent.MoveToFront(entry)
	return min(maximum, hint.size)
}

func (c *scanSizeCache) remember(key [sha256.Size]byte, size int) {
	if size < 1 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[key]; entry != nil {
		hint := entry.Value.(*scanSizeHint)
		hint.size = min(hint.size, size)
		c.recent.MoveToFront(entry)
		return
	}
	if c.entries == nil {
		c.entries = make(map[[sha256.Size]byte]*list.Element)
	}
	hint := &scanSizeHint{key: key, size: size, expires: time.Now().Add(time.Minute)}
	c.entries[key] = c.recent.PushFront(hint)
	if c.recent.Len() > maxScanSizeHints {
		oldest := c.recent.Back()
		delete(c.entries, oldest.Value.(*scanSizeHint).key)
		c.recent.Remove(oldest)
	}
}
