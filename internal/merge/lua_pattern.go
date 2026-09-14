package merge

import (
	"strings"

	"github.com/iceisfun/golua/vm"
)

func boundLuaPatterns(library *vm.Table) {
	library.SetString("find", vm.NewNativeFunc(boundedLuaFind))
	library.SetString("match", vm.NewNativeFunc(boundedLuaMatch))
	library.SetString("gmatch", vm.NewNativeFunc(boundedLuaGmatch))
}

type luaPatternInput struct {
	text    string
	pattern string
	start   int
}

func patternInput(state *vm.VM) luaPatternInput {
	if err := state.CheckInterrupt(); err != nil {
		panic(err)
	}
	input := luaPatternInput{text: substitutionString(state.Get(1)), pattern: substitutionString(state.Get(2))}
	position := int64(1)
	if value := state.Get(3); !value.IsNil() {
		var ok bool
		position, ok = value.ToInt()
		if !ok {
			panic("string pattern start must be an integer")
		}
	}
	if position < 0 {
		position += int64(len(input.text)) + 1
	}
	// Clamp before converting to int or adding one in the pattern matcher.
	input.start = int(min(max(1, position)-1, int64(len(input.text))+1))
	return input
}

func boundedLuaFind(state *vm.VM) int {
	input := patternInput(state)
	if input.start > len(input.text) {
		state.Set(0, vm.Nil)
		return 1
	}
	plain := state.Get(4).ToBool() || !strings.ContainsAny(input.pattern, "^$*+?.([%-")
	var start, end int
	var captures []captureValue
	var found bool
	if plain {
		index := strings.Index(input.text[input.start:], input.pattern)
		found = index >= 0
		start = input.start + index
		end = start + len(input.pattern)
	} else {
		start, end, captures, found = luaMatchFrom(state, input.text, input.pattern, input.start+1)
	}
	if !found {
		state.Set(0, vm.Nil)
		return 1
	}
	checkCaptures(captures)
	// Native return slots are not reserved by the caller's argument frame.
	state.EnsureStack(state.Base() + len(captures) + 1)
	state.Set(0, vm.NewInt(int64(start+1)))
	state.Set(1, vm.NewInt(int64(end)))
	for index, capture := range captures {
		state.Set(index+2, patternCapture(capture))
	}
	return 2 + len(captures)
}

func boundedLuaMatch(state *vm.VM) int {
	input := patternInput(state)
	start, end, captures, found := luaMatchFrom(state, input.text, input.pattern, input.start+1)
	if !found {
		state.Set(0, vm.Nil)
		return 1
	}
	return returnPatternMatch(state, input.text[start:end], captures)
}

func boundedLuaGmatch(state *vm.VM) int {
	input := patternInput(state)
	position, lastEnd := input.start, -1
	iterator := vm.NewNativeFunc(func(state *vm.VM) int {
		budget := &luaPatternBudget{state: state}
		for position <= len(input.text) {
			end, captures, found := luaMatchAt(budget, input.text, input.pattern, position)
			if found && end != lastEnd {
				start := position
				lastEnd = end
				if end == position {
					position++
				} else {
					position = end
				}
				return returnPatternMatch(state, input.text[start:end], captures)
			}
			position++
		}
		return 0
	})
	state.Set(0, iterator)
	return 1
}

func returnPatternMatch(state *vm.VM, whole string, captures []captureValue) int {
	checkCaptures(captures)
	state.EnsureStack(state.Base() + max(1, len(captures)) - 1)
	if len(captures) == 0 {
		state.Set(0, vm.NewString(whole))
		return 1
	}
	for index, capture := range captures {
		state.Set(index, patternCapture(capture))
	}
	return len(captures)
}

func patternCapture(capture captureValue) vm.Value {
	if capture.isPos {
		return vm.NewInt(int64(capture.pos))
	}
	return vm.NewString(capture.str)
}
