package app

import (
	"fmt"
	"math"
	"strings"

	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/config"
)

type memoryRequirement struct {
	name  string
	bytes int64
}

// These are startup sizing estimates, not allocation reservations or an OOM
// guarantee. Keep enough room below the high watermark for one normal work unit.
func minimumMemory(loaded config.Config) []memoryRequirement {
	requirements := []memoryRequirement{{name: "runtime and drivers", bytes: 64 << 20}}
	if loaded.Mode != config.ModeWorker {
		requestMemory := memoryRequirement{name: "request decoding", bytes: startupBytes(2, loaded.GRPC.MaxReceiveMessageBytes)}
		requirements = append(requirements, requestMemory)
		responseMemory := memoryRequirement{name: "response encoding", bytes: startupBytes(2, loaded.GRPC.MaxSendMessageBytes)}
		requirements = append(requirements, responseMemory)
	}
	if loaded.Mode != config.ModeGateway {
		mergeMemory := memoryRequirement{name: "merge documents", bytes: startupBytes(2, loaded.Service.Merge.Lua.MaxResultBytes)}
		requirements = append(requirements, mergeMemory)
		sourceCache := memoryRequirement{name: "Lua source cache", bytes: startupBytes(loaded.Service.Merge.Lua.MaxCachedPrograms, loaded.Service.Merge.Lua.MaxSourceBytes)}
		requirements = append(requirements, sourceCache)
		if loaded.Storage.Driver == config.DriverMongoDB {
			mongoMemory := memoryRequirement{name: "MongoDB wire buffers", bytes: 48 << 20}
			requirements = append(requirements, mongoMemory)
		}
		if loaded.Storage.Kafka.Enabled {
			if loaded.Mode == config.ModeEngine {
				producerMemory := memoryRequirement{name: "Kafka producer buffer", bytes: int64(loaded.Storage.Kafka.Producer.MaxBufferedBytes)}
				requirements = append(requirements, producerMemory)
			} else {
				fetchMemory := memoryRequirement{name: "Kafka fetch and decoded batch", bytes: 48 << 20}
				requirements = append(requirements, fetchMemory)
				deadLetterMemory := memoryRequirement{name: "Kafka dead-letter producer buffer", bytes: max(64<<20, int64(loaded.Storage.Kafka.MaxRecordBytes)+(16<<10))}
				requirements = append(requirements, deadLetterMemory)
			}
		}
	}
	return requirements
}

func newMemory(loaded config.Config) (*capacity.Guard, error) {
	bytes, source := capacity.Detect()
	if loaded.Memory.MaxBytes > 0 && (source == "fallback" || int64(loaded.Memory.MaxBytes) < bytes) {
		bytes, source = int64(loaded.Memory.MaxBytes), "configured"
	}
	requireStartupMemory(loaded, bytes, source)
	opts := capacity.Options{Bytes: bytes, HighPercent: loaded.Memory.HighWatermarkPercent, LowPercent: loaded.Memory.LowWatermarkPercent, Role: string(loaded.Mode), Store: loaded.Storage.Name, Source: source}
	return capacity.New(opts)
}

func requireStartupMemory(loaded config.Config, available int64, source string) {
	var minimum int64
	var breakdown []string
	for _, requirement := range minimumMemory(loaded) {
		minimum += min(requirement.bytes, math.MaxInt64-minimum)
		breakdown = append(breakdown, fmt.Sprintf("%s=%d bytes", requirement.name, requirement.bytes))
	}
	high := int64(loaded.Memory.HighWatermarkPercent)
	normal := available/100*high + available%100*high/100
	if normal < minimum {
		panic(fmt.Sprintf("insufficient startup memory for %s: available=%d bytes (%s), high watermark=%d bytes (%d%%), minimum working memory=%d bytes; %s", loaded.Mode, available, source, normal, high, minimum, strings.Join(breakdown, ", ")))
	}
}

func startupBytes(count, size int) int64 {
	if count <= 0 || size <= 0 {
		return 0
	}
	if int64(count) > math.MaxInt64/int64(size) {
		return math.MaxInt64
	}
	return int64(count) * int64(size)
}
