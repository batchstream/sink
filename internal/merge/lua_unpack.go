package merge

import (
	"strings"

	"github.com/iceisfun/golua/vm"
)

// The runtime writes unpack results directly to the native call's stack frame
// without growing it. Reserve the full return frame, including next-position,
// before invoking it. Even zero-width directives consume work and stack slots.
func prepareLuaUnpack(state *vm.VM) {
	if err := state.CheckInterrupt(); err != nil {
		panic(err)
	}
	format, err := luaPackString(state.Get(1))
	if err != nil {
		panic(err)
	}
	results := 1
	alignment := false
	for index := 0; index < len(format); index++ {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		kind := format[index]
		if kind >= '0' && kind <= '9' {
			continue
		}
		if alignment {
			alignment = false
			continue
		}
		switch kind {
		case 'X':
			alignment = true
		case ' ', '<', '>', '=', '!', 'x':
		default:
			if strings.ContainsRune("bBhHlLjJTiIfdnczs", rune(kind)) {
				results++
			}
		}
	}
	state.EnsureStack(state.Base() + results - 1)
}
