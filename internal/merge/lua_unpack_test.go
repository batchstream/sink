package merge

import (
	"context"
	"strings"
	"testing"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestLuaUnpackGrowsResultStack(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	boundLuaAllocations(state, 4096)
	function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("unpack")
	arguments := []vm.Value{vm.NewString(strings.Repeat("B", 2048)), vm.NewString(strings.Repeat("a", 2048))}
	results, err := state.ProtectedCall(function, arguments)
	if err != nil || len(results) != 2049 {
		t.Fatalf("valid unpack failed: results=%d error=%v", len(results), err)
	}
	if results[0].AsInt() != 97 || results[2047].AsInt() != 97 || results[2048].AsInt() != 2049 {
		t.Fatal("unpack corrupted results or next position")
	}
}

func TestLuaUnpackEnforcesStackLimit(t *testing.T) {
	limits := vm.Limits{MaxStackSlots: 128}
	state := vm.New(vm.WithLimits(limits))
	defer state.Close(context.Background())
	stdlib.Open(state)
	boundLuaAllocations(state, 4096)
	function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("unpack")
	arguments := []vm.Value{vm.NewString(strings.Repeat("c0", 256)), vm.NewString("")}
	_, err := state.ProtectedCall(function, arguments)
	if err == nil || !strings.Contains(err.Error(), "stack overflow") {
		t.Fatalf("unpack ignored stack limit: %v", err)
	}
}

func TestLuaUnpackChargesFormatWork(t *testing.T) {
	limits := vm.Limits{MaxInstructions: 20}
	state := vm.New(vm.WithLimits(limits))
	defer state.Close(context.Background())
	stdlib.Open(state)
	boundLuaAllocations(state, 4096)
	function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("unpack")
	arguments := []vm.Value{vm.NewString(strings.Repeat(" ", 100)), vm.NewString("")}
	_, err := state.ProtectedCall(function, arguments)
	if err == nil || !strings.Contains(err.Error(), "instruction limit exceeded") {
		t.Fatalf("unpack ignored instruction budget: %v", err)
	}
}

func TestLuaUnpackRetainsFormatSemantics(t *testing.T) {
	plain := vm.New()
	defer plain.Close(context.Background())
	stdlib.Open(plain)
	bounded := vm.New()
	defer bounded.Close(context.Background())
	stdlib.Open(bounded)
	boundLuaAllocations(bounded, 4096)
	plainLibrary := plain.GetGlobal("string").AsTable().(*vm.Table)
	boundedLibrary := bounded.GetGlobal("string").AsTable().(*vm.Table)
	for _, alignment := range []string{"", "!2 ", "!4 ", "!8 ", "!16 "} {
		for _, variable := range []string{"s", "s1", "s2", "s4", "s16", "z"} {
			format := alignment + "b " + variable + " Xs4 i4 d c3 c0 x"
			arguments := []vm.Value{vm.NewString(format), vm.NewInt(1), vm.NewString("abc"), vm.NewInt(-3), vm.NewFloat(1.5), vm.NewString("xyz"), vm.NewString("")}
			encoded, err := plain.ProtectedCall(plainLibrary.GetString("pack"), arguments)
			if err != nil {
				t.Fatal(err)
			}
			arguments = []vm.Value{vm.NewString(format), encoded[0]}
			want, err := plain.ProtectedCall(plainLibrary.GetString("unpack"), arguments)
			if err != nil {
				t.Fatal(err)
			}
			got, err := bounded.ProtectedCall(boundedLibrary.GetString("unpack"), arguments)
			if err != nil || len(got) != len(want) {
				t.Fatalf("%q: result count=%d want=%d error=%v", format, len(got), len(want), err)
			}
			for index := range want {
				if got[index].Type() != want[index].Type() || got[index].String() != want[index].String() {
					t.Fatalf("%q: result %d=%v want=%v", format, index, got[index], want[index])
				}
			}
		}
	}
}

func TestLuaUnpackHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	state := vm.New(vm.WithContext(ctx))
	defer state.Close(context.Background())
	stdlib.Open(state)
	boundLuaAllocations(state, 4096)
	cancel()
	function := state.GetGlobal("string").AsTable().(*vm.Table).GetString("unpack")
	arguments := []vm.Value{vm.NewString(""), vm.NewString("")}
	if _, err := state.ProtectedCall(function, arguments); err == nil {
		t.Fatal("unpack ignored cancellation")
	}
}
