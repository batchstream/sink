package merge

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/iceisfun/golua/vm"
)

func addUnicodeTextFunctions(luaVM *vm.VM, maximum int) {
	value := luaVM.GetGlobal("utf8")
	if !value.IsTable() {
		panic("Lua UTF-8 library is unavailable")
	}
	library, ok := value.AsTable().(*vm.Table)
	if !ok {
		panic("Lua UTF-8 library is not a concrete table")
	}
	library.SetString("upper", vm.NewNativeFunc(func(state *vm.VM) int {
		return unicodeUpper(state, maximum)
	}))
}

func unicodeUpper(state *vm.VM, maximum int) int {
	value := state.Get(1)
	if !value.IsString() {
		got := "no value"
		if state.ArgCount() >= 1 {
			got = value.Type()
		}
		panic(fmt.Sprintf("bad argument #1 to 'utf8.upper' (string expected, got %s)", got))
	}
	input := value.AsString()
	var output strings.Builder
	changed := false
	size := 0
	for offset, count := 0, 0; offset < len(input); count++ {
		if count%1024 == 0 {
			if err := state.CheckInterrupt(); err != nil {
				panic(err)
			}
		}
		character, width := utf8.DecodeRuneInString(input[offset:])
		upper := unicode.ToUpper(character)
		encoded := utf8.RuneLen(upper)
		if encoded > maximum-size {
			panic(nativeAllocationLimit)
		}
		size += encoded
		if !changed && (upper != character || encoded != width) {
			// Preserve the original string when conversion is a no-op, including
			// BSON datetime identity. Invalid UTF-8 becomes RuneError, as in Go.
			changed = true
			output.Grow(min(len(input), maximum))
			output.WriteString(input[:offset])
		}
		if changed {
			output.WriteRune(upper)
		}
		offset += width
	}
	if err := state.CheckInterrupt(); err != nil {
		panic(err)
	}
	if changed {
		state.Set(0, vm.NewString(output.String()))
	} else {
		state.Set(0, value)
	}
	return 1
}
