package service

import (
	"strings"
	"unicode/utf8"

	sink "github.com/batchstream/sink-protocol/sink/v1"
)

const maxFailureMessageBytes = 1024

func failureMessageLimit(operations, maximum int) int {
	// Even tiny document quotas must retain a nonempty protocol diagnostic.
	return min(maxFailureMessageBytes, max(1, maximum/max(1, operations)-128))
}

func boundedFailureMessage(message string, maximum int) string {
	if maximum <= 0 {
		return ""
	}
	suffix := ""
	if len(message) > maximum {
		suffix = "..."[:min(3, maximum)]
		end := maximum - len(suffix)
		for end > 0 && !utf8.RuneStart(message[end]) {
			end--
		}
		message = message[:end]
	}
	// A Lua error can contain binary strings. Retain valid protobuf UTF-8 and
	// detach shortened text from the potentially large original error string.
	return strings.Clone(strings.ToValidUTF8(message, "?")) + suffix
}

func boundResultFailures[T interface{ GetFailure() *sink.Failure }](results []T, budgets *responseGroups, maximum int) {
	counts := make([]int, budgets.callerCount())
	for index := range results {
		counts[budgets.owner(index)]++
	}
	for index, result := range results {
		if failure := result.GetFailure(); failure != nil {
			limit := failureMessageLimit(counts[budgets.owner(index)], maximum)
			failure.Message = boundedFailureMessage(failure.Message, limit)
		}
	}
}
