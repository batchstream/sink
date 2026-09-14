package merge

import (
	"errors"
	"fmt"
	"strings"

	"github.com/iceisfun/golua/vm"
)

const nativeAllocationLimit = "lua native allocation exceeds document byte limit"

// Preflight allocating library calls before the runtime builds their result.
// These per-call bounds complement, but do not provide, a VM heap quota.
func boundLuaAllocations(luaVM *vm.VM, maximum int) {
	addUnicodeTextFunctions(luaVM, maximum)
	stringsTable := luaVM.GetGlobal("string").AsTable().(*vm.Table)
	boundLuaPatterns(stringsTable)
	format := stringsTable.GetString("format")
	stringsTable.SetString("format", vm.NewNativeFunc(func(state *vm.VM) int {
		return boundedLuaFormat(state, format, maximum)
	}))
	find := stringsTable.GetString("find")
	stringsTable.SetString("gsub", vm.NewNativeFunc(func(state *vm.VM) int {
		substitution := luaSubstitution{state: state, find: find, maximum: maximum}
		return substitution.run()
	}))
	pack := stringsTable.GetString("pack")
	packSize := stringsTable.GetString("packsize")
	stringsTable.SetString("pack", vm.NewNativeFunc(func(state *vm.VM) int {
		arguments := luaArguments(state)
		format, err := sizedPackFormat(arguments)
		if err != nil {
			panic(err)
		}
		sizeArguments := []vm.Value{vm.NewString(format)}
		size, err := state.ProtectedCall(packSize, sizeArguments)
		if err != nil {
			panic(err)
		}
		if len(size) != 1 || !size[0].IsInt() || size[0].AsInt() < 0 || size[0].AsInt() > int64(maximum) {
			panic(nativeAllocationLimit)
		}
		return callLuaLibrary(state, pack, arguments)
	}))
	tables := luaVM.GetGlobal("table").AsTable().(*vm.Table)
	tables.SetString("concat", vm.NewNativeFunc(func(state *vm.VM) int {
		return boundedLuaConcat(state, maximum)
	}))
	tables.SetString("move", vm.NewNativeFunc(boundedLuaMove))
	tables.SetString("insert", vm.NewNativeFunc(boundedLuaInsert))
	tables.SetString("remove", vm.NewNativeFunc(boundedLuaRemove))
	tables.SetString("pack", vm.NewNativeFunc(boundedLuaPack))
	tables.SetString("unpack", vm.NewNativeFunc(boundedLuaUnpack))
	tables.SetString("sort", vm.NewNativeFunc(boundedLuaSort))
}

func luaArguments(state *vm.VM) []vm.Value {
	arguments := make([]vm.Value, state.ArgCount())
	for index := range arguments {
		arguments[index] = state.Get(index + 1)
	}
	return arguments
}

func callLuaLibrary(state *vm.VM, function vm.Value, arguments []vm.Value) int {
	results, err := state.ProtectedCall(function, arguments)
	if err != nil {
		panic(err)
	}
	for index, value := range results {
		state.Set(index, value)
	}
	return len(results)
}

func luaPackString(value vm.Value) (string, error) {
	if value.IsString() {
		return value.AsString(), nil
	}
	if value.IsNumber() {
		return value.String(), nil
	}
	return "", errors.New("string.pack requires a string or number argument")
}

// Replace variable-length directives with equally sized fixed directives, then
// let the runtime's packsize validate the complete format and its alignment.
// The original pack still performs encoding and value validation.
func sizedPackFormat(arguments []vm.Value) (string, error) {
	if len(arguments) == 0 {
		return "", errors.New("string.pack requires a format")
	}
	format, err := luaPackString(arguments[0])
	if err != nil {
		return "", err
	}
	var sized strings.Builder
	argument := 1
	alignment := false
	for index := 0; index < len(format); {
		start := index
		kind := format[index]
		index++
		if strings.ContainsRune("!iIcs", rune(kind)) {
			for index < len(format) && format[index] >= '0' && format[index] <= '9' {
				index++
			}
		}
		directive := format[start:index]
		if alignment {
			sized.WriteString(directive)
			alignment = false
			continue
		}
		switch kind {
		case 'X':
			alignment = true
		case ' ', '<', '>', '=', '!', 'x':
		case 's', 'z':
			if argument >= len(arguments) {
				return "", errors.New("string.pack is missing a string argument")
			}
			value, err := luaPackString(arguments[argument])
			if err != nil {
				return "", err
			}
			argument++
			length := len(value)
			if kind == 's' {
				prefix := directive[1:]
				if prefix == "" {
					prefix = "8"
				}
				sized.WriteString("i" + prefix)
			} else {
				length++
			}
			fmt.Fprintf(&sized, "c%d", length)
			continue
		default:
			argument++
		}
		sized.WriteString(directive)
	}
	return sized.String(), nil
}
