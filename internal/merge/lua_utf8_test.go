package merge

import (
	"context"
	"strings"
	"testing"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestLuaUnicodeUpperMatchesGoWithinExactBudget(t *testing.T) {
	inputs := []string{
		"", "ALREADY UPPER 123", "café κόσμος straße 中文", "ȿɀıſﬀ",
		"𐐨𐐩", "a\x00b", "\xff\xfea", "\xe2\x82", "\xef\xbf\xbd",
		strings.Repeat("ȿ", 400), strings.Repeat("a", 4096),
	}
	for _, input := range inputs {
		want := strings.ToUpper(input)
		state := vm.New()
		stdlib.Open(state)
		addUnicodeTextFunctions(state, len(want))
		function := state.GetGlobal("utf8").AsTable().(*vm.Table).GetString("upper")
		arguments := []vm.Value{vm.NewString(input)}
		got, err := state.ProtectedCall(function, arguments)
		state.Close(context.Background())
		if err != nil || len(got) != 1 || !got[0].IsString() || got[0].AsString() != want {
			t.Fatalf("upper(%q): got=%v want=%q error=%v", input, got, want, err)
		}
		if len(want) == 0 {
			continue
		}
		state = vm.New()
		stdlib.Open(state)
		addUnicodeTextFunctions(state, len(want)-1)
		function = state.GetGlobal("utf8").AsTable().(*vm.Table).GetString("upper")
		_, err = state.ProtectedCall(function, arguments)
		state.Close(context.Background())
		if err == nil || !strings.Contains(err.Error(), nativeAllocationLimit) {
			t.Fatalf("upper(%q) escaped byte budget: %v", input, err)
		}
	}
}

func TestLuaUnicodeUpperHonorsExecutionLimits(t *testing.T) {
	for _, interrupt := range []string{"cancel", "instructions"} {
		t.Run(interrupt, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			limits := vm.Limits{MaxInstructions: 2}
			state := vm.New(vm.WithContext(ctx), vm.WithLimits(limits))
			defer state.Close(context.Background())
			stdlib.Open(state)
			addUnicodeTextFunctions(state, 16<<10)
			function := state.GetGlobal("utf8").AsTable().(*vm.Table).GetString("upper")
			want := "instruction limit exceeded"
			if interrupt == "cancel" {
				cancel()
				want = "context canceled"
			}
			arguments := []vm.Value{vm.NewString(strings.Repeat("a", 8<<10))}
			_, err := state.ProtectedCall(function, arguments)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("uppercase ignored %s: %v", interrupt, err)
			}
		})
	}
}
