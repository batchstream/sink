package merge

import (
	"context"
	"strings"
	"testing"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestLuaPatternCapturesHonorStackLimit(t *testing.T) {
	for _, name := range []string{"find", "match", "gmatch"} {
		t.Run(name, func(t *testing.T) {
			limits := vm.Limits{MaxStackSlots: 16}
			state := vm.New(vm.WithLimits(limits))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 4096)
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString(name)
			arguments := []vm.Value{vm.NewString(""), vm.NewString(strings.Repeat("()", 32))}
			results, err := state.ProtectedCall(function, arguments)
			if name == "gmatch" && err == nil {
				results, err = state.ProtectedCall(results[0], nil)
			}
			if err == nil || !strings.Contains(err.Error(), "stack overflow") {
				t.Fatalf("captures escaped the return stack limit: results=%d error=%v", len(results), err)
			}
		})
	}
}

func TestLuaPatternCapturesGrowReturnStack(t *testing.T) {
	for _, name := range []string{"find", "match", "gmatch"} {
		t.Run(name, func(t *testing.T) {
			state := vm.New()
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 4096)
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString(name)
			arguments := []vm.Value{vm.NewString(""), vm.NewString(strings.Repeat("()", 32))}
			if name == "gmatch" {
				results, err := state.ProtectedCall(function, arguments)
				if err != nil {
					t.Fatal(err)
				}
				function, arguments = results[0], nil
			}
			// Keep the nested native frame near the end of the initial VM stack.
			// Its arguments fit, while the complete capture results need growth.
			outer := vm.NewNativeFunc(func(state *vm.VM) int {
				results, err := state.ProtectedCall(function, arguments)
				if err != nil {
					panic(err)
				}
				want := 32
				if name == "find" {
					want += 2
				}
				if len(results) != want {
					t.Fatalf("results=%d want=%d", len(results), want)
				}
				for index, value := range results {
					expected := int64(1)
					if name == "find" && index == 1 {
						expected = 0
					}
					if !value.IsInt() || value.AsInt() != expected {
						t.Fatalf("capture result %d=%v want=%d", index, value, expected)
					}
				}
				return 0
			})
			padding := make([]vm.Value, 240)
			if _, err := state.ProtectedCall(outer, padding); err != nil {
				t.Fatalf("valid pattern captures failed at stack boundary: %v", err)
			}
		})
	}
}
