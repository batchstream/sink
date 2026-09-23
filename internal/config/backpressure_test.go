package config

import (
	"fmt"
	"strings"
	"testing"

	"github.com/batchstream/sink/internal/backpressure"
)

func TestStoreConcurrencyCeilingIndependentOfRoleAndMemory(t *testing.T) {
	for _, mode := range []string{"engine", "worker"} {
		base := "mode: " + mode + "\n"
		if mode == "worker" {
			base += "consumer: {group_id: workers}\n"
		}
		loaded, err := Decode(strings.NewReader(base), strings.NewReader(kafkaStore))
		if err != nil || loaded.Service.StoreMaxConcurrent != backpressure.DefaultMaxConcurrent {
			t.Fatalf("%s default: %+v %v", mode, loaded.Service, err)
		}
		for _, maximum := range []int{-1, 0, 1, 128, 4096, 4097} {
			storeConfig := fmt.Sprintf("name: primary\nmax_concurrent: %d\n", maximum)
			store := strings.Replace(kafkaStore, "name: primary\n", storeConfig, 1)
			loaded, err := Decode(strings.NewReader(base), strings.NewReader(store))
			valid := maximum >= 1 && maximum <= 4096
			if valid && (err != nil || loaded.Service.StoreMaxConcurrent != maximum) || !valid && err == nil {
				t.Fatalf("%s maximum=%d: %v", mode, maximum, err)
			}
		}
	}
}
