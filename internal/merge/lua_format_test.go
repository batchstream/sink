package merge

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestBoundedFormatMatchesRuntime(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	library := state.GetGlobal("string").AsTable().(*vm.Table)
	plain := library.GetString("format")
	boundLuaAllocations(state, 4096)
	bounded := library.GetString("format")
	binary := make([]byte, 256)
	for index := range binary {
		binary[index] = byte(index)
	}
	values := []vm.Value{vm.Nil, vm.True, vm.NewInt(0), vm.NewInt(-1), vm.NewInt(math.MinInt64),
		vm.NewFloat(math.MaxFloat64), vm.NewFloat(math.Inf(1)), vm.NewFloat(math.NaN()),
		vm.NewString(""), vm.NewString("é中abc"), vm.NewString("123"), vm.NewString(string(binary)),
		vm.NewString("\x001\x1f2\x7f3\n\"\\"), vm.NewTable(vm.NewTableWithSize(0, 0))}
	for _, spec := range []string{"%s", "%q", "%d", "%i", "%u", "%x", "%X", "%o", "%c", "%p", "%f", "%e", "%E", "%g", "%G", "%a", "%A", "%99s", "%--5.2s", "%.s", "%-10s", "%.2s", "%+.99f", "%#.0x", "%05d", "%%", "%", "%100s", "%100d", "%.100s", "%1q", "%F", "%v", "%1%", "%1.2.3s"} {
		for _, value := range values {
			for _, format := range []string{spec, "prefix%%" + spec + "|" + spec + "suffix"} {
				arguments := []vm.Value{vm.NewString(format), value, value}
				want, wantErr := state.ProtectedCall(plain, arguments)
				got, gotErr := state.ProtectedCall(bounded, arguments)
				if (wantErr == nil) != (gotErr == nil) {
					t.Fatalf("%q value=%v: runtime=%v bounded=%v", format, value, wantErr, gotErr)
				}
				if wantErr == nil && (len(got) != 1 || !got[0].IsString() || got[0].AsString() != want[0].AsString()) {
					t.Fatalf("%q value=%v: got=%v want=%v", format, value, got, want)
				}
			}
		}
	}
}

func TestBoundedFormatStopsBeforeExcessConversions(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	plain := state.GetGlobal("string").AsTable().(*vm.Table).GetString("format")
	calls := 0
	formatter := vm.NewNativeFunc(func(state *vm.VM) int {
		calls++
		return callLuaLibrary(state, plain, luaArguments(state))
	})
	bounded := vm.NewNativeFunc(func(state *vm.VM) int { return boundedLuaFormat(state, formatter, 10) })
	for _, spec := range []string{"%s%s", "%q%s"} {
		calls = 0
		arguments := []vm.Value{vm.NewString(spec), vm.NewString("abcde"), vm.NewString("abcdef")}
		_, err := state.ProtectedCall(bounded, arguments)
		if err == nil || !strings.Contains(err.Error(), nativeAllocationLimit) || calls != 1 {
			t.Fatalf("%s: excess conversion ran: calls=%d error=%v", spec, calls, err)
		}
	}
}

func TestBoundedFormatExactByteLimits(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	formatter := state.GetGlobal("string").AsTable().(*vm.Table).GetString("format")
	binary := make([]byte, 256)
	for index := range binary {
		binary[index] = byte(index)
	}
	for _, spec := range []string{"%s", "%q", "prefix:%q%%", "%99s", "%--5.2s", "%.s"} {
		for _, value := range []string{"", "é中abc", string(binary), "\x001\x1f2\x7f3\n\"\\"} {
			arguments := []vm.Value{vm.NewString(spec), vm.NewString(value)}
			want, err := state.ProtectedCall(formatter, arguments)
			if err != nil {
				continue
			}
			length := len(want[0].AsString())
			for _, maximum := range []int{length, length - 1} {
				bounded := vm.NewNativeFunc(func(state *vm.VM) int { return boundedLuaFormat(state, formatter, maximum) })
				got, err := state.ProtectedCall(bounded, arguments)
				if maximum == length {
					if err != nil || got[0].AsString() != want[0].AsString() {
						t.Fatalf("%s rejected its exact %d-byte budget: %v", spec, maximum, err)
					}
				} else if err == nil || !strings.Contains(err.Error(), nativeAllocationLimit) {
					t.Fatalf("%s exceeded its %d-byte budget: %v", spec, maximum, err)
				}
			}
		}
	}
}

func TestBoundedFormatHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	state := vm.New(vm.WithContext(ctx))
	defer state.Close(context.Background())
	stdlib.Open(state)
	formatter := vm.NewNativeFunc(func(state *vm.VM) int {
		cancel()
		state.Set(0, vm.NewString("a"))
		return 1
	})
	bounded := vm.NewNativeFunc(func(state *vm.VM) int { return boundedLuaFormat(state, formatter, 10) })
	arguments := []vm.Value{vm.NewString("%s%s"), vm.NewString("a"), vm.NewString("a")}
	_, err := state.ProtectedCall(bounded, arguments)
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("format ignored cancellation: %v", err)
	}
}
