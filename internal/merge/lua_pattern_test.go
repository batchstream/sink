package merge

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestLuaPatternsHonorInstructionLimit(t *testing.T) {
	for _, name := range []string{"find", "match", "gmatch", "gsub"} {
		t.Run(name, func(t *testing.T) {
			limits := vm.Limits{MaxInstructions: 10}
			state := vm.New(vm.WithLimits(limits))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 4096)
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString(name)
			arguments := []vm.Value{vm.NewString(strings.Repeat("a", 256)), vm.NewString("a*b")}
			if name == "gsub" {
				arguments = append(arguments, vm.NewString("x"))
			}
			results, err := state.ProtectedCall(function, arguments)
			if name == "gmatch" && err == nil {
				_, err = state.ProtectedCall(results[0], nil)
			}
			if err == nil || !strings.Contains(err.Error(), "instruction limit exceeded") {
				t.Fatalf("%s pattern search escaped the instruction budget: %v", name, err)
			}
		})
	}
}

func TestLuaPatternsHonorInFlightCancellation(t *testing.T) {
	for _, name := range []string{"find", "match", "gmatch", "gsub"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			state := vm.New(vm.WithContext(ctx))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 4096)
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString(name)
			arguments := []vm.Value{vm.NewString(strings.Repeat("a", 256)), vm.NewString("a*a*a*a*a*b")}
			if name == "gsub" {
				arguments = append(arguments, vm.NewString("x"))
			}
			if name == "gmatch" {
				results, err := state.ProtectedCall(function, arguments)
				if err != nil {
					t.Fatal(err)
				}
				function, arguments = results[0], nil
			}
			timer := time.AfterFunc(5*time.Millisecond, cancel)
			defer timer.Stop()
			started := time.Now()
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("%s did not stop its active pattern search: %v", name, err)
			}
			if elapsed := time.Since(started); elapsed > time.Second {
				t.Fatalf("%s took %s to observe cancellation", name, elapsed)
			}
		})
	}
}

func TestLuaPatternInnerLoopsChargeWork(t *testing.T) {
	for _, tc := range []struct {
		name    string
		input   string
		pattern string
	}{
		{name: "greedy", input: strings.Repeat("a", 8192), pattern: "^a*"},
		{name: "lazy", input: strings.Repeat("a", 8192), pattern: "^a-b"},
		{name: "balanced", input: "(" + strings.Repeat("a", 8192) + ")", pattern: "^%b()"},
		{name: "same delimiter", input: "|" + strings.Repeat("a", 8192) + "|", pattern: "^%b||"},
		{name: "set parsing", input: "z", pattern: "^[" + strings.Repeat("a", 8192) + "]"},
		{name: "set matching", input: strings.Repeat("z", 512), pattern: "^[" + strings.Repeat("a", 128) + "z]*"},
		{name: "frontier parsing", input: "z", pattern: "^%f[" + strings.Repeat("a", 8192) + "]"},
		{name: "tail folding", pattern: "^" + strings.Repeat("a*", 8192)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limits := vm.Limits{MaxInstructions: 10}
			state := vm.New(vm.WithLimits(limits))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 16384)
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("find")
			arguments := []vm.Value{vm.NewString(tc.input), vm.NewString(tc.pattern)}
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || !strings.Contains(err.Error(), "instruction limit exceeded") {
				t.Fatalf("%s ignored native work: %v", tc.name, err)
			}
		})
	}
	// Seed a completed capture to isolate the backreference byte comparison.
	limits := vm.Limits{MaxInstructions: 10}
	state := vm.New(vm.WithLimits(limits))
	defer state.Close(context.Background())
	budget := &luaPatternBudget{state: state}
	matcher := &matchState{budget: budget, s: strings.Repeat("a", 16384), p: "%1", level: 1}
	matcher.cap[0] = capSlot{init: 0, slen: 8192}
	function := vm.NewNativeFunc(func(_ *vm.VM) int {
		matcher.matchBackRef(8192, 0)
		return 0
	})
	_, err := state.ProtectedCall(function, nil)
	if err == nil || !strings.Contains(err.Error(), "instruction limit exceeded") {
		t.Fatalf("backreference ignored native work: %v", err)
	}
}

func TestBoundedPatternsMatchRuntime(t *testing.T) {
	plain := vm.New()
	defer plain.Close(context.Background())
	stdlib.Open(plain)
	bounded := vm.New()
	defer bounded.Close(context.Background())
	stdlib.Open(bounded)
	boundLuaAllocations(bounded, 16384)
	inputs := []string{"", "abc", "a)b", "ababa", "  a  b ", "a\x00b", "123 abc 456", "((abc))", "|abc|", "é中", "\xff\x00"}
	patterns := []string{"", ".", ".*", ".-", "a", "a*", "a?", "a+b", "%w+", "^a", "^", "$", "%f[%w]%w+", "%b()", "%b||", "()", "(%a+)", "(.)()", "(%a*)()", "(%w+)%s*(%w*)", "(a)%1", "[a-z]+", "[^a]+", "[]a]", "[%A%z]+", "a)b", ")", "(", "%", "[", "%f", "%b", "%0", "%1", "()%1"}
	for _, input := range inputs {
		for _, pattern := range patterns {
			for _, start := range []string{"nil", "0", "1", "2", "-1", "-20", "20", `"2"`, "math.mininteger", "math.maxinteger"} {
				for _, name := range []string{"find", "match", "gmatch"} {
					source := fmt.Sprintf("return string.%s(%q, %q, %s)", name, input, pattern, start)
					if name == "gmatch" {
						source = fmt.Sprintf(`local iterator=string.gmatch(%q, %q, %s)
local results={}
for i=1,32 do
 local values=table.pack(iterator())
 if values.n==0 then break end
 for j=1,values.n do results[#results+1]=type(values[j])..":"..tostring(values[j]) end
end
return table.concat(results,"|")`, input, pattern, start)
					}
					checkPatternParity(t, plain, bounded, source)
				}
			}
		}
	}
	for _, source := range []string{
		`return string.find("a*b", "*", 1, true)`,
		`return string.find("a)b", ")", 1, 0)`,
		`return string.match(123.0, 23)`,
		`return string.find("abc", nil)`,
		`return string.match("abc", ".", 1.5)`,
		`return string.match("abc", ".", {})`,
		`return ("abc"):match("(.*)")`,
	} {
		checkPatternParity(t, plain, bounded, source)
	}
}

func checkPatternParity(t *testing.T, plain, bounded *vm.VM, source string) {
	t.Helper()
	block, err := parser.Parse("pattern-parity", source)
	if err != nil {
		t.Fatal(err)
	}
	program, err := compiler.Compile("pattern-parity", block)
	if err != nil {
		t.Fatal(err)
	}
	want, wantErr := plain.Run(program)
	got, gotErr := bounded.Run(program)
	if (wantErr == nil) != (gotErr == nil) {
		t.Fatalf("%s: runtime=%v bounded=%v", source, wantErr, gotErr)
	}
	if wantErr != nil {
		return
	}
	if len(want) != len(got) {
		t.Fatalf("%s: got=%v want=%v", source, got, want)
	}
	for index := range want {
		if got[index].Type() != want[index].Type() || got[index].String() != want[index].String() {
			t.Fatalf("%s: got=%v want=%v", source, got, want)
		}
	}
}
