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
			input := base + fmt.Sprintf("execution: {store_max_concurrent: %d}\n", maximum)
			loaded, err := Decode(strings.NewReader(input), strings.NewReader(kafkaStore))
			valid := maximum >= 1 && maximum <= 4096
			if valid && (err != nil || loaded.Service.StoreMaxConcurrent != maximum) || !valid && err == nil {
				t.Fatalf("%s maximum=%d: %v", mode, maximum, err)
			}
		}
	}
}
