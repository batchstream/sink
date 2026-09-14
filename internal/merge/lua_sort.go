// Adapted from github.com/iceisfun/golua/stdlib/table.go at v1.1.1.
// Copyright 2026 github.com/iceisfun. See lua_pattern.LICENSE.
// Keep the runtime's comparison order and invalid-comparator checks while
// charging native work without allocating a callback frame per comparison.

package merge

import "github.com/iceisfun/golua/vm"

func boundedLuaSort(state *vm.VM) int {
	defer state.EnterNonYieldable()()
	table := state.Get(1)
	if !table.IsTable() {
		panic("table.sort requires a table")
	}
	comparator := state.Get(2)
	length, err := state.ObjLen(table)
	if err != nil {
		panic(err)
	}
	if length <= 1 {
		return 0
	}
	if length > 1<<30 {
		panic("table.sort array too big")
	}
	if !comparator.IsNil() && !comparator.IsFunction() && !comparator.IsNativeFunc() {
		panic("table.sort comparator must be a function")
	}
	sorter := luaTableSorter{state: state, table: table, comparator: comparator}
	sorter.sort(1, length)
	return 0
}

type luaTableSorter struct {
	state      *vm.VM
	table      vm.Value
	comparator vm.Value
}

func (s *luaTableSorter) get(index int) vm.Value {
	value, err := s.state.IndexInt(s.table, index)
	if err != nil {
		panic(err)
	}
	return value
}

func (s *luaTableSorter) set(index int, value vm.Value) {
	if err := s.state.SetIndexInt(s.table, index, value); err != nil {
		panic(err)
	}
}

func (s *luaTableSorter) less(left, right vm.Value) bool {
	if err := s.state.CheckInterrupt(); err != nil {
		panic(err)
	}
	if s.comparator.IsNil() {
		less, err := s.state.CompareLT(left, right)
		if err != nil {
			failure := &vm.LuaError{Value: vm.NewString(err.Error())}
			panic(failure)
		}
		return less
	}
	arguments := []vm.Value{left, right}
	results, err := s.state.ProtectedCall(s.comparator, arguments)
	if err != nil {
		panic(err)
	}
	return len(results) > 0 && results[0].ToBool()
}

func (s *luaTableSorter) sort(first, last int) {
	for first < last {
		left, right := s.get(first), s.get(last)
		if s.less(right, left) {
			s.set(first, right)
			s.set(last, left)
		}
		if last-first == 1 {
			return
		}
		middle := (first + last) / 2
		pivot := s.get(middle)
		left = s.get(first)
		if s.less(pivot, left) {
			s.set(first, pivot)
			s.set(middle, left)
		} else {
			right = s.get(last)
			if s.less(right, pivot) {
				s.set(middle, right)
				s.set(last, pivot)
			}
		}
		if last-first == 2 {
			return
		}
		pivot = s.get(middle)
		s.set(middle, s.get(last-1))
		s.set(last-1, pivot)
		lower, upper := first, last-1
		for {
			for {
				lower++
				if !s.less(s.get(lower), pivot) {
					break
				}
				if lower == last-1 {
					panic("invalid order function for sorting")
				}
			}
			for {
				upper--
				if !s.less(pivot, s.get(upper)) {
					break
				}
				if upper < lower {
					panic("invalid order function for sorting")
				}
			}
			if upper < lower {
				s.set(last-1, s.get(lower))
				s.set(lower, pivot)
				break
			}
			left, right = s.get(lower), s.get(upper)
			s.set(lower, right)
			s.set(upper, left)
		}
		// Recurse on the smaller partition to keep stack use logarithmic.
		if lower-first < last-lower {
			s.sort(first, lower-1)
			first = lower + 1
		} else {
			s.sort(lower+1, last)
			last = lower - 1
		}
	}
}
