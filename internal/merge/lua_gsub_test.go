package merge

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestBoundedGsubMatchesRuntime(t *testing.T) {
	plain := vm.New()
	defer plain.Close(context.Background())
	stdlib.Open(plain)
	bounded := vm.New()
	defer bounded.Close(context.Background())
	stdlib.Open(bounded)
	boundLuaAllocations(bounded, 4096)
	for _, input := range []string{"", "abc", "ababa", "  a  b ", "a\x00b", "123 abc 456", "(abc)", "é中"} {
		for _, pattern := range []string{"", ".", ".*", ".-", "a", "a*", "a?", "%w+", "^a", "^", "$", "%f[%w]%w+", "%b()", "()", "(%a+)", "(.)()", "(%a*)()", "(%w+)%s*(%w*)", "(a)%1"} {
			for _, replacement := range []string{`""`, `"x"`, `"%0"`, `"[%1]"`, `"%%-%0-%1"`, `123.0`, `function(x) return x end`, `function(x) return false end`, `function() end`, `function(x) return tostring(x).."!", "ignored" end`, `{a="X", b=false, [1]=42}`, `setmetatable({}, {__index=function(_, k) return tostring(k).."!" end})`} {
				for _, limit := range []string{"nil", "0", "1", "2", "-1", `"3"`} {
					source := fmt.Sprintf("return string.gsub(%q, %q, %s, %s)", input, pattern, replacement, limit)
					block, err := parser.Parse("gsub-parity", source)
					if err != nil {
						t.Fatal(err)
					}
					compiled, err := compiler.Compile("gsub-parity", block)
					if err != nil {
						t.Fatal(err)
					}
					want, wantErr := plain.Run(compiled)
					got, gotErr := bounded.Run(compiled)
					if (wantErr == nil) != (gotErr == nil) {
						t.Fatalf("%s: runtime=%v bounded=%v", source, wantErr, gotErr)
					}
					if wantErr != nil {
						continue
					}
					if len(got) != len(want) {
						t.Fatalf("%s: result count %d want %d", source, len(got), len(want))
					}
					for index := range want {
						if got[index].Type() != want[index].Type() || got[index].String() != want[index].String() {
							t.Fatalf("%s: result %d=%v want %v", source, index, got[index], want[index])
						}
					}
				}
			}
		}
	}
}

func TestBoundedGsubStopsBeforeExcessCallbacks(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	boundLuaAllocations(state, 10)
	function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("gsub")
	calls := 0
	replacement := vm.NewNativeFunc(func(state *vm.VM) int {
		calls++
		state.Set(0, vm.NewString("abcde"))
		return 1
	})
	arguments := []vm.Value{vm.NewString("aaaa"), vm.NewString("."), replacement}
	_, err := state.ProtectedCall(function, arguments)
	if err == nil || !strings.Contains(err.Error(), nativeAllocationLimit) || calls != 3 {
		t.Fatalf("unbounded callbacks: %d err=%v", calls, err)
	}
	arguments[0] = vm.NewString("aa")
	result, err := state.ProtectedCall(function, arguments)
	if err != nil || result[0].AsString() != "abcdeabcde" || result[1].AsInt() != 2 {
		t.Fatalf("exact boundary failed: %v %v", result, err)
	}
}

func TestBoundedGsubHonorsCancellationAndInstructions(t *testing.T) {
	for _, interrupt := range []string{"cancel", "instructions"} {
		t.Run(interrupt, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			limits := vm.Limits{MaxInstructions: 10}
			state := vm.New(vm.WithContext(ctx), vm.WithLimits(limits))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 1024)
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("gsub")
			replacement := vm.NewString("b")
			if interrupt == "cancel" {
				replacement = vm.NewNativeFunc(func(state *vm.VM) int {
					cancel()
					state.Set(0, vm.NewString("b"))
					return 1
				})
			}
			arguments := []vm.Value{vm.NewString(strings.Repeat("a", 100)), vm.NewString("."), replacement}
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || (!strings.Contains(err.Error(), "context canceled") && !strings.Contains(err.Error(), "instruction limit exceeded")) {
				t.Fatalf("native loop did not stop: %v", err)
			}
		})
	}
}
