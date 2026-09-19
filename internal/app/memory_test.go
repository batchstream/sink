package app

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/liran/sink/internal/config"
)

func TestStartupMemoryMinimumForEveryRole(t *testing.T) {
	for _, role := range []string{"gateway", "engine", "worker"} {
		t.Run(role, func(t *testing.T) {
			component := "mode: " + role + "\n"
			shared := strings.NewReader("name: primary\nstorage: {driver: mongodb, mongodb: {uri: 'mongodb://localhost:27017'}}\nkafka: {enabled: true, brokers: [localhost:9092], topic: {name: mutations}}")
			if role == "gateway" {
				component += "forwarding: {routes: [{store: primary, target: 'localhost:8080', tls: {insecure: true}}]}\n"
				shared = nil
			}
			if role == "worker" {
				component += "consumer: {group_id: workers}\n"
			}
			// A typed nil io.Reader is not an absent Store document.
			var loaded config.Config
			var err error
			if shared == nil {
				loaded, err = config.Decode(strings.NewReader(component), nil)
			} else {
				loaded, err = config.Decode(strings.NewReader(component), shared)
			}
			if err != nil {
				t.Fatal(err)
			}
			var minimum int64
			for _, item := range minimumMemory(loaded) {
				minimum += item.bytes
			}
			enough := (minimum*100 + 79) / 80
			requireStartupMemory(loaded, enough, "test")
			defer func() {
				message := fmt.Sprint(recover())
				for _, part := range []string{"insufficient startup memory", role, "available=", "minimum working memory=", "runtime and drivers="} {
					if !strings.Contains(message, part) {
						t.Fatalf("missing startup diagnostic %q: %s", part, message)
					}
				}
			}()
			requireStartupMemory(loaded, enough-1, "test")
		})
	}
}

func TestStartupMemoryEstimateCannotOverflow(t *testing.T) {
	loaded, err := config.Decode(strings.NewReader("mode: gateway\nforwarding: {routes: [{store: primary, target: 'localhost:8080', tls: {insecure: true}}]}"), nil)
	if err != nil {
		t.Fatal(err)
	}
	loaded.GRPC.MaxReceiveMessageBytes = math.MaxInt
	loaded.GRPC.MaxSendMessageBytes = math.MaxInt
	defer func() {
		if recover() == nil {
			t.Fatal("overflow bypassed the startup memory check")
		}
	}()
	requireStartupMemory(loaded, math.MaxInt64, "test")
}
