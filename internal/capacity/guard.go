// Package capacity gates new work using process memory high and low watermarks.
package capacity

import (
	"context"
	"errors"
	"io/fs"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const SampleInterval = 100 * time.Millisecond
const DefaultHighPercent = 80
const DefaultLowPercent = 70

var ErrBusy = status.Error(codes.ResourceExhausted, "process memory is above its admission watermark")

type Options struct {
	Bytes       int64
	HighPercent int
	LowPercent  int
	Role        string
	Store       string
	Source      string
	// Files supplies the operating-system memory counters; nil uses the host filesystem.
	Files fs.FS
}

type Guard struct {
	mu          sync.Mutex
	limit       int64
	high        int64
	low         int64
	used        int64
	blocked     bool
	sampled     time.Time
	files       fs.FS
	usageSource string
	limitSource string
	observed    *observations
}

type contextKey struct{}

func New(opts Options) (*Guard, error) {
	if opts.Bytes < 1024 || opts.HighPercent < 1 || opts.HighPercent > 99 || opts.LowPercent < 1 || opts.LowPercent >= opts.HighPercent {
		return nil, errors.New("memory requires at least 1KiB and 0 < low_watermark_percent < high_watermark_percent < 100")
	}
	files := opts.Files
	if files == nil {
		files = os.DirFS("/")
	}
	guard := &Guard{limit: opts.Bytes, high: percent(opts.Bytes, opts.HighPercent), low: percent(opts.Bytes, opts.LowPercent), files: files, limitSource: opts.Source}
	guard.observed = newObservations(opts)
	return guard, nil
}

func percent(bytes int64, value int) int64 {
	return bytes/100*int64(value) + bytes%100*int64(value)/100
}

// Admit checks once at the process boundary. Already admitted work is never
// rechecked while queued, executing, returning a response, or retrying a conflict.
func (g *Guard) Admit(ctx context.Context) (context.Context, error) {
	if err := ctx.Err(); err != nil {
		return ctx, status.FromContextError(err).Err()
	}
	if g == nil || ctx.Value(contextKey{}) == g {
		return ctx, nil
	}
	_, blocked, _ := g.snapshot()
	if blocked {
		g.observed.rejected.Inc()
		return ctx, ErrBusy
	}
	g.observed.admitted.Inc()
	return context.WithValue(ctx, contextKey{}, g), nil
}

func (g *Guard) Blocked() bool {
	if g == nil {
		return false
	}
	_, blocked, _ := g.snapshot()
	return blocked
}

// Wait is used before a Worker polls its next batch; it never interrupts a batch.
func (g *Guard) Wait(ctx context.Context) error {
	if g == nil {
		return ctx.Err()
	}
	ticker := time.NewTicker(SampleInterval)
	defer ticker.Stop()
	for g.Blocked() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return ctx.Err()
}

func (g *Guard) snapshot() (int64, bool, string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.sampled.IsZero() || time.Since(g.sampled) >= SampleInterval {
		g.used, g.usageSource = processUsage(g.files)
		g.sampled = time.Now()
		if g.used >= g.high {
			g.blocked = true
		} else if g.used <= g.low {
			g.blocked = false
		}
	}
	return g.used, g.blocked, g.usageSource
}

func processUsage(files fs.FS) (int64, string) {
	if raw, err := fs.ReadFile(files, "proc/self/statm"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) >= 2 {
			pages, err := strconv.ParseInt(fields[1], 10, 64)
			if err == nil && pages >= 0 && pages <= math.MaxInt64/int64(os.Getpagesize()) {
				return pages * int64(os.Getpagesize()), "rss"
			}
		}
	}
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(min(stats.Sys-stats.HeapReleased, math.MaxInt64)), "go_runtime"
}

func (g *Guard) Limit() int64 { return g.limit }
