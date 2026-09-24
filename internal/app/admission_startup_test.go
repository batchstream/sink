package app

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/batchstream/sink/internal/backpressure"
	"github.com/batchstream/sink/internal/config"
)

type startupListener struct {
	entered chan time.Time
	closed  chan struct{}
	once    sync.Once
}

func (l *startupListener) Accept() (net.Conn, error) {
	l.entered <- time.Now()
	<-l.closed
	return nil, net.ErrClosed
}

func (l *startupListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *startupListener) Addr() net.Addr {
	address := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
	return address
}

func TestEngineCompletesStoreStartupBeforeServingReadiness(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := backpressure.Options{Store: "primary", Role: "engine", MaxConcurrent: 64}
		controller, err := backpressure.New(opts)
		if err != nil {
			t.Fatal(err)
		}
		listener := &startupListener{entered: make(chan time.Time, 1), closed: make(chan struct{})}
		healthServer := &http.Server{Handler: http.NotFoundHandler()}
		settings := config.Config{Mode: config.ModeEngine, ShutdownTimeout: time.Second}
		app := &Application{config: settings, admission: controller, healthListener: listener, healthServer: healthServer}
		defer app.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- app.Run(ctx) }()
		synctest.Wait()
		time.Sleep(400 * time.Millisecond)
		select {
		case <-listener.entered:
			t.Fatal("readiness was served before Store startup completed")
		default:
		}
		served := <-listener.entered
		if served.Sub(start) < 500*time.Millisecond || served.Sub(start) > 1500*time.Millisecond {
			t.Fatalf("startup staggering changed: %s", served.Sub(start))
		}
		var permits []*backpressure.Permit
		for range 4 {
			permit, _, _ := controller.TryAcquire()
			if permit == nil {
				t.Fatal("ready Engine still has a zero/one startup window")
			}
			permits = append(permits, permit)
		}
		for _, permit := range permits {
			permit.Release()
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestEngineStartupHonorsProcessCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := backpressure.Options{Store: "primary", Role: "engine"}
		controller, err := backpressure.New(opts)
		if err != nil {
			t.Fatal(err)
		}
		listener := &startupListener{entered: make(chan time.Time, 1), closed: make(chan struct{})}
		healthServer := &http.Server{Handler: http.NotFoundHandler()}
		settings := config.Config{Mode: config.ModeEngine, ShutdownTimeout: time.Second}
		app := &Application{config: settings, admission: controller, healthListener: listener, healthServer: healthServer}
		defer app.Close()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- app.Run(ctx) }()
		synctest.Wait()
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		select {
		case <-listener.entered:
			t.Fatal("canceled Engine started serving during initialization")
		default:
		}
	})
}
