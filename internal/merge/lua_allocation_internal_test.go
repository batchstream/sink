package merge

import (
	"context"
	"strings"
	"testing"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestLuaPackChargesFormatWork(t *testing.T) {
	for _, name := range []string{"pack", "packsize"} {
		t.Run(name, func(t *testing.T) {
			limits := vm.Limits{MaxInstructions: 20}
			state := vm.New(vm.WithLimits(limits))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 4096)
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString(name)
			arguments := []vm.Value{vm.NewString(strings.Repeat(" ", 100))}
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || !strings.Contains(err.Error(), "instruction limit exceeded") {
				t.Fatalf("%s ignored instruction budget: %v", name, err)
			}
		})
	}
}

func TestLuaPackHonorsCancellation(t *testing.T) {
	for _, name := range []string{"pack", "packsize"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			state := vm.New(vm.WithContext(ctx))
			defer state.Close(context.Background())
			stdlib.Open(state)
			boundLuaAllocations(state, 4096)
			cancel()
			function := state.GetGlobal("string").AsTable().(*vm.Table).GetString(name)
			arguments := []vm.Value{vm.NewString("")}
			if _, err := state.ProtectedCall(function, arguments); err == nil {
				t.Fatalf("%s ignored cancellation", name)
			}
		})
	}
}

func TestPackPreflightMatchesRuntimeEncodingSize(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	library := state.GetGlobal("string").AsTable().(*vm.Table)
	pack := library.GetString("pack")
	packSize := library.GetString("packsize")
	for _, alignment := range []string{"", "!2 ", "!4 ", "!8 ", "!16 "} {
		for _, fixed := range []string{"b", "B", "h", "H", "l", "L", "j", "J", "T", "i", "I", "i16", "I16", "f", "d", "n"} {
			for _, variable := range []string{"s", "s1", "s2", "s4", "s16", "z"} {
				format := alignment + "b " + variable + " " + fixed + " " + variable + " Xs4 c3 x"
				t.Run(format, func(t *testing.T) {
					arguments := []vm.Value{vm.NewString(format), vm.NewInt(1), vm.NewString("abc"), vm.NewInt(2), vm.NewString("123456789"), vm.NewString("xyz")}
					encoded, err := state.ProtectedCall(pack, arguments)
					if err != nil {
						t.Fatal(err)
					}
					normalized, err := sizedPackFormat(arguments)
					if err != nil {
						t.Fatal(err)
					}
					sizeArguments := []vm.Value{vm.NewString(normalized)}
					size, err := state.ProtectedCall(packSize, sizeArguments)
					if err != nil || len(size) != 1 || size[0].AsInt() != int64(len(encoded[0].AsString())) {
						t.Fatalf("preflight %q disagrees with encoded size %d: %v err=%v", normalized, len(encoded[0].AsString()), size, err)
					}
				})
			}
		}
	}
}
