package service

import (
	"strings"
	"unicode/utf8"

	sink "github.com/liran/sink/gen/sink"
)

const maxFailureMessageBytes = 1024

// Reserve error text as well as result envelopes before parsing or executing.
// This also covers failures derived from stored documents, not just RPC input.
func failureResponseBytes(operations int) int {
	return operations * (maxFailureMessageBytes + 128)
}

func failureMessageLimit(operations, maximum int) int {
	return min(maxFailureMessageBytes, max(0, maximum/max(1, operations)-128))
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

func boundResultFailures[T interface{ GetFailure() *sink.Failure }](results []T, budgets *requestBudgets, maximum int) {
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
