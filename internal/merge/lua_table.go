package merge

import (
	"fmt"
	"math"
	"strings"

	"github.com/iceisfun/golua/vm"
)

// Native table loops must charge work even when their values are empty or nil.
// VM bytecode checkpoints cannot interrupt a standard-library call in progress.
func boundedLuaMove(state *vm.VM) int {
	defer state.EnterNonYieldable()()
	first := luaTableInteger(state.Get(2))
	last := luaTableInteger(state.Get(3))
	target := luaTableInteger(state.Get(4))
	source := state.Get(1)
	destination := state.Get(5)
	if destination.IsNil() {
		destination = source
	}
	if !source.IsTable() || !destination.IsTable() {
		panic("table.move requires source and destination tables")
	}
	if first > last {
		state.Set(0, destination)
		return 1
	}
	count := uint64(last) - uint64(first) + 1
	if count == 0 || count > math.MaxInt64 {
		panic("table.move has too many elements to move")
	}
	if target > math.MaxInt64-int64(count-1) {
		panic("table.move destination wraps around")
	}
	backward := source.RawEqual(destination) && target > first && target <= last
	for index := int64(0); index < int64(count); index++ {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		offset := index
		if backward {
			offset = int64(count) - 1 - index
		}
		value, err := state.IndexInt(source, int(first+offset))
		if err != nil {
			panic(err)
		}
		if err := state.SetIndexInt(destination, int(target+offset), value); err != nil {
			panic(err)
		}
	}
	state.Set(0, destination)
	return 1
}

func luaTableInteger(value vm.Value) int64 {
	integer, ok := value.ToInt()
	if !ok {
		panic("table range requires integer arguments")
	}
	return integer
}

func boundedLuaConcat(state *vm.VM, maximum int) int {
	defer state.EnterNonYieldable()()
	table := state.Get(1)
	if !table.IsTable() {
		panic("table.concat requires a table")
	}
	separator := ""
	if value := state.Get(2); !value.IsNil() {
		var err error
		separator, err = luaPackString(value)
		if err != nil {
			panic(err)
		}
	}
	first := int64(1)
	if value := state.Get(3); !value.IsNil() {
		first = luaTableInteger(value)
	}
	lastValue := state.Get(4)
	var last int64
	if !lastValue.IsNil() {
		last = luaTableInteger(lastValue)
	}
	length, err := state.ObjLen(table)
	if err != nil {
		panic(err)
	}
	if lastValue.IsNil() {
		last = int64(length)
	}
	var output strings.Builder
	for index := first; index <= last; index++ {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		value, err := state.IndexInt(table, int(index))
		if err != nil {
			panic(err)
		}
		part, err := luaPackString(value)
		if err != nil {
			panic(fmt.Sprintf("invalid value at index %d in table.concat: %v", index, err))
		}
		prefix := ""
		if index != first {
			prefix = separator
		}
		if len(prefix) > maximum-output.Len() || len(part) > maximum-output.Len()-len(prefix) {
			panic(nativeAllocationLimit)
		}
		if first == last {
			// strings.Join preserves the backing string for one element. Keep
			// that identity for directly propagated BSON datetime values too.
			state.Set(0, vm.NewString(part))
			return 1
		}
		output.WriteString(prefix)
		output.WriteString(part)
		if index == last {
			break
		}
	}
	state.Set(0, vm.NewString(output.String()))
	return 1
}
