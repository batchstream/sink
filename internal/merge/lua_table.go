// Native table operations adapted from github.com/iceisfun/golua/stdlib/table.go
// at v1.1.1. Copyright 2026 github.com/iceisfun. See lua_pattern.LICENSE.

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

func boundedLuaInsert(state *vm.VM) int {
	defer state.EnterNonYieldable()()
	table := state.Get(1)
	if !table.IsTable() || state.ArgCount() < 2 || state.ArgCount() > 3 {
		panic("table.insert requires a table and a value, with an optional position")
	}
	count := state.ArgCount()
	positionValue, value := state.Get(2), state.Get(2)
	if count == 3 {
		value = state.Get(3)
	}
	length, err := state.ObjLen(table)
	if err != nil {
		panic(err)
	}
	position := int64(length) + 1
	if count == 3 {
		position = luaTableInteger(positionValue)
		if position < 1 || position-1 > int64(length) {
			panic("table.insert position out of bounds")
		}
	}
	for index := int64(length) + 1; index > position; index-- {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		shifted, err := state.IndexInt(table, int(index-1))
		if err != nil {
			panic(err)
		}
		if err := state.SetIndexInt(table, int(index), shifted); err != nil {
			panic(err)
		}
	}
	if err := state.CheckInterrupt(); err != nil {
		panic(err)
	}
	if err := state.SetIndexInt(table, int(position), value); err != nil {
		panic(err)
	}
	return 0
}

func boundedLuaRemove(state *vm.VM) int {
	defer state.EnterNonYieldable()()
	table := state.Get(1)
	if !table.IsTable() {
		panic("table.remove requires a table")
	}
	positionValue := state.Get(2)
	length, err := state.ObjLen(table)
	if err != nil {
		panic(err)
	}
	position := int64(length)
	if !positionValue.IsNil() {
		position = luaTableInteger(positionValue)
	}
	if position != int64(length) && (position < 1 || position-1 > int64(length)) {
		panic("table.remove position out of bounds")
	}
	if position > int64(length) {
		state.Set(0, vm.Nil)
		return 1
	}
	removed, err := state.IndexInt(table, int(position))
	if err != nil {
		panic(err)
	}
	for index := position; index < int64(length); index++ {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		shifted, err := state.IndexInt(table, int(index+1))
		if err != nil {
			panic(err)
		}
		if err := state.SetIndexInt(table, int(index), shifted); err != nil {
			panic(err)
		}
	}
	if err := state.CheckInterrupt(); err != nil {
		panic(err)
	}
	if err := state.SetIndexInt(table, length, vm.Nil); err != nil {
		panic(err)
	}
	state.Set(0, removed)
	return 1
}

func boundedLuaPack(state *vm.VM) int {
	count := state.ArgCount()
	table := vm.NewTableWithSize(count, 1)
	table.EnsureArraySize(count)
	for index := 1; index <= count; index++ {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		table.RawSetArray(index, state.Get(index))
	}
	table.ShrinkArray()
	table.SetString("n", vm.NewInt(int64(count)))
	state.Set(0, vm.NewTable(table))
	return 1
}

func boundedLuaUnpack(state *vm.VM) int {
	defer state.EnterNonYieldable()()
	table := state.Get(1)
	first := int64(1)
	if value := state.Get(2); !value.IsNil() {
		first = luaTableInteger(value)
	}
	last := int64(0)
	if value := state.Get(3); !value.IsNil() {
		last = luaTableInteger(value)
	} else {
		length, err := state.ObjLen(table)
		if err != nil {
			panic(err)
		}
		last = int64(length)
	}
	if first > last {
		return 0
	}
	count := uint64(last) - uint64(first) + 1
	if count == 0 || count > 1_000_000 || !state.CheckStack(state.Base()+int(count)) {
		panic("too many results to unpack")
	}
	state.EnsureStack(state.Base() + int(count))
	// Metamethod calls use the result slots too, so collect every value before
	// copying them to the VM's return frame, as the original library does.
	results := make([]vm.Value, int(count))
	for offset := range results {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		value, err := state.IndexInt(table, int(first+int64(offset)))
		if err != nil {
			panic(err)
		}
		results[offset] = value
	}
	for index, value := range results {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		state.Set(index, value)
	}
	return len(results)
}
