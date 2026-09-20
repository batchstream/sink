package merge_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/sink/internal/merge"
)

func TestLuaEnvironmentIsFreshAfterMutationAndFailure(t *testing.T) {
	source := []byte(`
return function(current, incoming)
    assert(marker == nil)
    assert(string.upper("hello") == "HELLO")
    assert(("hello"):upper() == "HELLO")
    assert(math.max(1, 2) == 2)
    assert(table.concat({"a", "b"}) == "ab")
    assert(utf8.upper("café") == "CAFÉ")
    assert(string.format("%s:%d", "ok", 2) == "ok:2")
    assert(("ab"):gsub(".", "x") == "xx")
    assert(#string.pack("c4", "a") == 4)
    assert(string.unpack("c1", "x") == "x")
    assert(string.packsize("c4") == 4)
    assert(json.is_null(json.null))
    assert(type(json.object()) == "table" and type(json.array()) == "table")
    assert(_G == nil and io == nil and os == nil and package == nil)
    assert(load == nil and require == nil and debug == nil)
    assert(math.random == nil and string.rep == nil)
    local observed = sink.v1.time.now()
    marker = true
    string.upper = function() return "leaked" end
    string.format = nil
    string.gsub = nil
    string.pack = nil
    string.unpack = nil
    string.packsize = nil
    math.max = nil
    table.concat = nil
    utf8.upper = nil
    sink.v1.time.now = function() return "leaked" end
    json.null = {}
    json.object = nil
    json.array = nil
    json.is_null = nil
    if incoming.fail then error("deliberate failure") end
    return {observed=observed}
end`)
	options := merge.LuaOptions{}
	merger := compileTestProgram(t, source, options)
	base := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	failed := merge.Request{Incoming: jsonDocument(`{"fail":true}`), ObservedAt: base}
	if _, err := merger.Merge(t.Context(), failed); err == nil {
		t.Fatal("deliberate script failure succeeded")
	}
	var executions sync.WaitGroup
	for index := range 64 {
		executions.Go(func() {
			observed := base.Add(time.Duration(index) * time.Second)
			request := merge.Request{Incoming: jsonDocument(`{}`), ObservedAt: observed}
			result, err := merger.Merge(t.Context(), request)
			if err != nil {
				t.Error(err)
				return
			}
			var document map[string]string
			if err := json.Unmarshal(result.Document.Payload, &document); err != nil {
				t.Error(err)
			}
			if document["observed"] != observed.Format(time.RFC3339Nano) {
				t.Errorf("request-bound time leaked: %s", result.Document.Payload)
			}
		})
	}
	executions.Wait()
}

func TestLuaEnvironmentKeepsEngineAllocationLimits(t *testing.T) {
	source := []byte(`return function(current, incoming)
    local scratch = string.pack("c600", "")
    return {ok=true}
end`)
	for _, maximum := range []int{256, 1024} {
		options := merge.LuaOptions{MaxResultBytes: maximum}
		merger := compileTestProgram(t, source, options)
		t.Run(fmt.Sprint(maximum), func(t *testing.T) {
			t.Parallel()
			for range 20 {
				request := merge.Request{Incoming: jsonDocument(`{}`)}
				result, err := merger.Merge(t.Context(), request)
				if maximum == 256 {
					if !errors.Is(err, merge.ErrExecutionExhausted) || len(result.Document.Payload) != 0 {
						t.Fatalf("small engine lost its native allocation limit: result=%s err=%v", result.Document.Payload, err)
					}
				} else if err != nil || string(result.Document.Payload) != `{"ok":true}` {
					t.Fatalf("large engine inherited a different limit: result=%s err=%v", result.Document.Payload, err)
				}
			}
		})
	}
}
