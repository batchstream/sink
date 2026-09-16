//go:build integration

package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	forward "github.com/liran/sink/gen/forward"
	sink "github.com/liran/sink/gen/sink"
	"golang.org/x/net/dns/dnsmessage"
	"google.golang.org/grpc"
)

// Real loopback DNS discovers a healthy new Engine and drains a removed one.
// No process-global resolver settings or Kubernetes APIs are used.
func TestGatewayDiscoversDNSScaleChanges(t *testing.T) {
	ipv4, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ipv4.Close() })
	_, port, err := net.SplitHostPort(ipv4.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	ipv6, err := net.Listen("tcp6", net.JoinHostPort("::1", port))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ipv6.Close() })
	listeners := []net.Listener{ipv4, ipv6}
	var backends []*controlledEngine
	var connections []*dnsCountingListener
	for _, listener := range listeners {
		backend := &controlledEngine{store: "a"}
		server := grpc.NewServer()
		forward.RegisterEngineServer(server, backend)
		counted := &dnsCountingListener{Listener: listener}
		go func() { _ = server.Serve(counted) }()
		t.Cleanup(server.Stop)
		backends = append(backends, backend)
		connections = append(connections, counted)
	}
	dnsAddress, state := startScalingDNS(t)
	fixture := fixtureEngine{store: "a", target: "dns://" + dnsAddress + "/" + net.JoinHostPort("engine.test", port)}
	gateway := testGateway(t, 4096, fixture)
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	request := &sink.WriteRequest{CompletionMode: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Operations: []*sink.WriteOperation{put("a", "one", false)}}
	attempts := int64(0)
	write := func() {
		t.Helper()
		attempts++
		request.Operations[0] = put("a", fmt.Sprintf("key-%d", attempts), false)
		response, err := gateway.Write(ctx, request)
		if err != nil || response.Results[0].Status != sink.WriteStatus_WRITE_STATUS_APPLIED {
			t.Fatalf("write: %v %v", response, err)
		}
	}
	write()
	if backends[0].calls.Load() != 1 || backends[1].calls.Load() != 0 {
		t.Fatal("initial route did not select IPv4")
	}
	state.stage.Store(1)
	deadline := time.Now().Add(10 * time.Second)
	for backends[1].calls.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("healthy scale-out was not discovered")
		}
		write()
		time.Sleep(20 * time.Millisecond)
	}
	first, second := backends[0].calls.Load(), backends[1].calls.Load()
	for range 100 {
		write()
	}
	if backends[0].calls.Load()-first < 25 || backends[1].calls.Load()-second < 25 {
		t.Fatal("record hashes did not distribute across Engine replicas")
	}
	state.stage.Store(3)
	deadline = time.Now().Add(10 * time.Second)
	for state.failedQueries.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("DNS refresh did not query failing resolver")
		}
		write()
		time.Sleep(20 * time.Millisecond)
	}
	for range 20 {
		write()
	}
	state.stage.Store(2)
	deadline = time.Now().Add(10 * time.Second)
	stable := 0
	for stable < 30 {
		if time.Now().After(deadline) {
			t.Fatal("removed Engine still receives traffic")
		}
		before := backends[0].calls.Load()
		write()
		if backends[0].calls.Load() == before {
			stable++
		} else {
			stable = 0
		}
		time.Sleep(20 * time.Millisecond)
	}
	if backends[0].calls.Load()+backends[1].calls.Load() != attempts {
		t.Fatal("mutation was replayed or lost")
	}
	for _, listener := range connections {
		if listener.accepts.Load() != 1 {
			t.Fatal("DNS refresh reconnected a healthy Engine")
		}
	}
	t.Logf("%d writes, healthy scale-out/scale-in and SERVFAIL: zero replays or reconnects", attempts)
}

type dnsCountingListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *dnsCountingListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err == nil {
		l.accepts.Add(1)
	}
	return connection, err
}

type scalingDNSState struct {
	stage         atomic.Int32
	failedQueries atomic.Int32
}

// Stage 0 serves IPv4 only, 1 serves both loopback backends, and 2 serves IPv6
// only. Stage 3 returns SERVFAIL. The long answer TTL verifies that scheduled
// refreshes query the designated server instead of reusing a local TTL cache.
func startScalingDNS(t *testing.T) (string, *scalingDNSState) {
	t.Helper()
	listener, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	state := &scalingDNSState{}
	done := make(chan struct{})
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
	})
	go func() {
		defer close(done)
		buffer := make([]byte, 4096)
		for {
			length, peer, err := listener.ReadFrom(buffer)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					t.Errorf("read DNS query: %v", err)
				}
				return
			}
			query := dnsmessage.Message{}
			if err := query.Unpack(buffer[:length]); err != nil {
				t.Errorf("parse DNS query: %v", err)
				return
			}
			response := dnsmessage.Message{
				Header: dnsmessage.Header{
					ID: query.ID, Response: true, Authoritative: true,
					RecursionDesired: query.RecursionDesired, RecursionAvailable: true,
				},
				Questions: query.Questions,
			}
			stage := state.stage.Load()
			if stage == 3 {
				response.RCode = dnsmessage.RCodeServerFailure
				state.failedQueries.Add(1)
			}
			for _, question := range query.Questions {
				header := dnsmessage.ResourceHeader{Name: question.Name, Type: question.Type, Class: dnsmessage.ClassINET, TTL: 300}
				var body dnsmessage.ResourceBody
				switch {
				case question.Type == dnsmessage.TypeA && stage < 2:
					body = &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}}
				case question.Type == dnsmessage.TypeAAAA && stage > 0 && stage < 3:
					body = &dnsmessage.AAAAResource{AAAA: [16]byte{15: 1}}
				}
				if body != nil {
					answer := dnsmessage.Resource{Header: header, Body: body}
					response.Answers = append(response.Answers, answer)
				}
			}
			packet, err := response.Pack()
			if err != nil {
				t.Errorf("encode DNS answer: %v", err)
				return
			}
			if _, err := listener.WriteTo(packet, peer); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("send DNS answer: %v", err)
				return
			}
		}
	}()
	return listener.LocalAddr().String(), state
}
