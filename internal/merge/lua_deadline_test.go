package merge_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liran/sink/internal/merge"
)

func TestLuaMergeDeadlineIncludesDocumentConversion(t *testing.T) {
	cases := []struct {
		name     string
		source   string
		incoming string
		current  string
	}{
		{
			name:     "decode input",
			source:   `return function(current, incoming) return {ok=true} end`,
			incoming: `{}` + strings.Repeat(" ", 8<<20),
		},
		{
			name:     "decode current",
			source:   `return function(current, incoming) return {ok=true} end`,
			incoming: `{}`,
			current:  `{}` + strings.Repeat(" ", 8<<20),
		},
		{
			name:     "encode result",
			source:   `return function(current, incoming) return {value=string.pack("c2097152", "")} end`,
			incoming: `{}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := merge.LuaOptions{Timeout: 5 * time.Millisecond}
			merger := compileTestProgram(t, []byte(tc.source), opts)
			request := merge.Request{Incoming: jsonDocument(tc.incoming)}
			if tc.current != "" {
				current := jsonDocument(tc.current)
				request.Current = &current
			}
			result, err := merger.Merge(t.Context(), request)
			if !errors.Is(err, merge.ErrExecutionDeadline) || len(result.Document.Payload) != 0 {
				t.Fatalf("document conversion escaped execution deadline: bytes=%d error=%v", len(result.Document.Payload), err)
			}
			// These are valid documents and scripts when conversion fits the
			// configured deadline, not invalid input or an allocation failure.
			opts.Timeout = 5 * time.Second
			merger = compileTestProgram(t, []byte(tc.source), opts)
			result, err = merger.Merge(t.Context(), request)
			if err != nil || len(result.Document.Payload) == 0 {
				t.Fatalf("conversion within its deadline failed: bytes=%d error=%v", len(result.Document.Payload), err)
			}
		})
	}
}
