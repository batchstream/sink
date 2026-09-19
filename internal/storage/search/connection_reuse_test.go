package search

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const connectionBurstSize = 16

func connectionBurstStore(t testing.TB, client *http.Client) (*Store, *atomic.Int64, chan struct{}, chan struct{}) {
	t.Helper()
	arrived, proceed := make(chan struct{}, connectionBurstSize), make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-proceed:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	})
	connections := new(atomic.Int64)
	backend := httptest.NewUnstartedServer(handler)
	backend.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	backend.Start()
	t.Cleanup(backend.Close)
	opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}, HTTPClient: client}
	store, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	t.Cleanup(func() { close(proceed) })
	return store, connections, arrived, proceed
}

func pingBurst(t testing.TB, store *Store, arrived <-chan struct{}, proceed chan<- struct{}) {
	t.Helper()
	finished := make(chan error, connectionBurstSize)
	for range connectionBurstSize {
		go func() { finished <- store.Ping(t.Context()) }()
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range connectionBurstSize {
		select {
		case <-arrived:
		case <-timer.C:
			t.Fatal("concurrent requests did not reach the backend")
		}
	}
	for range connectionBurstSize {
		proceed <- struct{}{}
	}
	for range connectionBurstSize {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSearchReusesConnectionsAcrossConcurrentBursts(t *testing.T) {
	store, connections, arrived, proceed := connectionBurstStore(t, nil)
	for range 3 {
		pingBurst(t, store, arrived, proceed)
	}
	if got := connections.Load(); got != connectionBurstSize {
		t.Fatalf("opened %d connections for three %d-request bursts; want reuse of the initial burst", got, connectionBurstSize)
	}
}

func BenchmarkSearchConnectionReuse(b *testing.B) {
	for _, name := range []string{"http-default", "store-pool"} {
		b.Run(name, func(b *testing.B) {
			var client *http.Client
			if name == "http-default" {
				transport := http.DefaultTransport.(*http.Transport).Clone()
				b.Cleanup(transport.CloseIdleConnections)
				client = &http.Client{Transport: transport, Timeout: defaultRequestTimeout}
			}
			store, connections, arrived, proceed := connectionBurstStore(b, client)
			pingBurst(b, store, arrived, proceed)
			initial := connections.Load()
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				pingBurst(b, store, arrived, proceed)
			}
			b.ReportMetric(float64(connections.Load()-initial)/float64(b.N), "connections/burst")
		})
	}
}

func TestSearchCloseOwnsOnlyItsDefaultPool(t *testing.T) {
	for _, custom := range []bool{false, true} {
		name := "owned"
		if custom {
			name = "caller-owned"
		}
		t.Run(name, func(t *testing.T) {
			var connections atomic.Int64
			closed := make(chan struct{}, 4)
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
			backend := httptest.NewUnstartedServer(handler)
			backend.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				} else if state == http.StateClosed {
					closed <- struct{}{}
				}
			}
			backend.Start()
			t.Cleanup(backend.Close)
			opts := Options{Driver: DriverOpenSearch, Store: "primary", Endpoints: []string{backend.URL}}
			if custom {
				transport := http.DefaultTransport.(*http.Transport).Clone()
				t.Cleanup(transport.CloseIdleConnections)
				opts.HTTPClient = &http.Client{Transport: transport}
			}
			first, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(first.Close)
			second, err := New(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(second.Close)
			for _, store := range []*Store{first, second} {
				if err := store.Ping(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			want := int64(2)
			if custom {
				want = 1
			}
			if got := connections.Load(); got != want {
				t.Fatalf("opened %d connections, want %d for two Stores", got, want)
			}
			first.Close()
			first.Close()
			if !custom {
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("Store close did not release its idle connection")
				}
			}
			if err := second.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := connections.Load(); got != want {
				t.Fatalf("closing one Store forced another client to reconnect: %d connections", got)
			}
		})
	}
}
