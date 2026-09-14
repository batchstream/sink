package merge_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/liran/sink/internal/merge"
)

func TestLuaBoundsNativeIntermediateResults(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "gsub string", body: `local value = string.pack("c600", ""); local scratch = string.gsub(value, ".", value)`},
		{name: "gsub captures", body: `local value = string.pack("c600", ""); local scratch = string.gsub(value, "(.*)", "%1%1")`},
		{name: "gsub callback", body: `local value = string.pack("c600", ""); local scratch = string.gsub("ab", ".", function() return value end)`},
		{name: "gsub table", body: `local value = string.pack("c600", ""); local scratch = string.gsub("aa", ".", {a=value})`},
		{name: "fixed pack", body: `local scratch = string.pack("c16777216", "")`},
		{name: "combined pack", body: `local scratch = string.pack("c600c600", "", "")`},
		{name: "variable pack", body: `local value = string.pack("c600", ""); local scratch = string.pack("s2z", value, value)`},
		{name: "concat", body: `local value = string.pack("c600", ""); local scratch = table.concat({value, value})`},
		{name: "concat numeric bounds", body: `local value = string.pack("c600", ""); local scratch = table.concat({value, value}, "", "1", "2")`},
		{name: "concat separator", body: `local value = string.pack("c600", ""); local scratch = table.concat({"a", "b", "c"}, value)`},
		{name: "format repeated strings", body: `local value = string.pack("c600", ""); local scratch = string.format("%s%s", value, value)`},
		{name: "format quoted string", body: `local value = string.pack("c600", ""); local scratch = string.format("%q", value)`},
		{name: "format modifiers", body: `local value = string.gsub(string.pack("c600", ""), "%z", "a"); local scratch = string.format("%-1s%-1s", value, value)`},
		{name: "format literal suffix", body: `local value = string.pack("c1024", ""); local scratch = string.format("%s!", value)`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := merge.LuaOptions{MaxResultBytes: 1024}
			source := []byte("return function(current, incoming) " + test.body + "; return {ok=true} end")
			merger := compileTestProgram(t, source, options)
			request := merge.Request{Incoming: jsonDocument(`{}`)}
			_, err := merger.Merge(t.Context(), request)
			if !errors.Is(err, merge.ErrExecutionExhausted) {
				t.Fatalf("unbounded native result: %v", err)
			}
		})
	}
}

func TestLuaTableLoopsExhaustMergeBudget(t *testing.T) {
	for _, body := range []string{
		`table.move({}, 1, 100, 1)`,
		`table.concat(incoming.values)`,
	} {
		opts := merge.LuaOptions{MaxInstructions: 50}
		source := []byte("return function(current, incoming) " + body + "; return {ok=true} end")
		merger := compileTestProgram(t, source, opts)
		incoming := jsonDocument(`{"values":[` + strings.Repeat(`"",`, 99) + `""]}`)
		request := merge.Request{Incoming: incoming}
		result, err := merger.Merge(t.Context(), request)
		if !errors.Is(err, merge.ErrExecutionExhausted) || len(result.Document.Payload) != 0 {
			t.Fatalf("native table loop escaped merge budget: result=%s error=%v", result.Document.Payload, err)
		}
	}
}

func TestLuaUnicodeUpperBoundsDiscardedIntermediateResult(t *testing.T) {
	opts := merge.LuaOptions{MaxResultBytes: 1024}
	source := []byte(`return function(current, incoming)
    local scratch = utf8.upper(incoming.value)
    return {ok=true}
end`)
	merger := compileTestProgram(t, source, opts)
	// U+023F occupies two UTF-8 bytes; its uppercase U+2C7E occupies three.
	incoming := jsonDocument(`{"value":"` + strings.Repeat("ȿ", 400) + `"}`)
	request := merge.Request{Incoming: incoming}
	result, err := merger.Merge(t.Context(), request)
	if !errors.Is(err, merge.ErrExecutionExhausted) || len(result.Document.Payload) != 0 {
		t.Fatalf("uppercase intermediate escaped byte budget: result=%s error=%v", result.Document.Payload, err)
	}
}

func TestLuaBoundedLibrariesPreserveNormalCalls(t *testing.T) {
	source := []byte(`return function(current, incoming)
    local packed = string.pack("!8 b s2 z Xh h c3", 1, "abc", "xyz", 2, "end")
    local a, b, c, d, e = string.unpack("!8 b s2 z Xh h c3", packed)
    assert(a == 1 and b == "abc" and c == "xyz" and d == 2 and e == "end")
    assert(#string.pack("c1024", "") == 1024)
    assert(table.concat({"a", "b", "c"}, ":", 2, 3) == "b:c")
    assert(table.concat({[0] = 1, [1] = 2}, "-", 0, 1) == "1-2")
    assert(table.concat({}, "", 2, 1) == "")
    assert(not pcall(string.pack, "c1025", ""))
    assert(not pcall(string.pack, "z", {}))
    assert(not pcall(table.concat, {true}))
    assert(#string.format("%s", string.pack("c1024", "")) == 1024)
    assert(#string.format("%q", string.pack("c511", "")) == 1024)
    assert(string.format("%.2s", "abcd") == "ab")
    assert(type(string.gsub(123, "x", "y")) == "string")
    assert(type(string.gsub(123, ".", "%0")) == "string")
    assert(type(string.gsub(123, ".", function() return false end)) == "string")
    return {ok = true}
end`)
	options := merge.LuaOptions{MaxResultBytes: 1024}
	merger := compileTestProgram(t, source, options)
	request := merge.Request{Incoming: jsonDocument(`{}`)}
	result, err := merger.Merge(t.Context(), request)
	if err != nil || string(result.Document.Payload) != `{"ok":true}` {
		t.Fatalf("result=%s err=%v", result.Document.Payload, err)
	}
}
