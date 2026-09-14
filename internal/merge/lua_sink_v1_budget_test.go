package merge_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/liran/sink/internal/merge"
)

func TestSinkV1UtilitiesShareInstructionBudgetAcrossCalls(t *testing.T) {
	for _, call := range []string{
		`array.append_all(json.array(), incoming.values)`,
		`array.deduplicate(incoming.values, tostring)`,
		`array.keep_tail(incoming.values, 40)`,
		`array.union_strings(incoming.values, nil)`,
		`object.replace_nonempty_array(json.object(), incoming, "values")`,
	} {
		t.Run(call, func(t *testing.T) {
			for _, count := range []int{1, 8} {
				source := fmt.Sprintf(`return function(current, incoming)
    local array, object = sink.v1.array, sink.v1.object
    for i = 1, %d do %s end
    return incoming
end`, count, call)
				opts := merge.LuaOptions{MaxInstructions: 200}
				merger := compileTestProgram(t, []byte(source), opts)
				incoming := jsonDocument(`{"values":[` + strings.Repeat(`"",`, 39) + `""]}`)
				request := merge.Request{Incoming: incoming}
				result, err := merger.Merge(t.Context(), request)
				if count == 1 {
					if err != nil || len(result.Document.Payload) == 0 {
						t.Fatalf("one small call should fit the budget: %v", err)
					}
				} else if !errors.Is(err, merge.ErrExecutionExhausted) || len(result.Document.Payload) != 0 {
					t.Fatalf("repeated calls escaped the shared instruction budget: result=%s error=%v", result.Document.Payload, err)
				}
			}
		})
	}
}
