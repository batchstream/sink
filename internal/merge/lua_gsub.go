package merge

import (
	"fmt"
	"strings"

	"github.com/iceisfun/golua/vm"
)

// Use the runtime's pattern matcher, but bound every append, including capture
// expansion, before allocating the replacement or accumulated output.
type luaSubstitution struct {
	state   *vm.VM
	find    vm.Value
	maximum int
	output  strings.Builder
}

func (s *luaSubstitution) append(value string) {
	if err := s.state.CheckInterrupt(); err != nil {
		panic(err)
	}
	if len(value) > s.maximum-s.output.Len() {
		panic(nativeAllocationLimit)
	}
	s.output.WriteString(value)
}

func (s *luaSubstitution) expand(replacement, whole string, captures []vm.Value) {
	for len(replacement) > 0 {
		index := strings.IndexByte(replacement, '%')
		if index < 0 {
			s.append(replacement)
			return
		}
		s.append(replacement[:index])
		replacement = replacement[index+1:]
		if len(replacement) == 0 {
			panic("invalid use of '%' in replacement string")
		}
		capture := replacement[0]
		replacement = replacement[1:]
		switch {
		case capture == '%':
			s.append("%")
		case capture == '0':
			s.append(whole)
		case capture >= '1' && capture <= '9':
			index := int(capture - '1')
			if index >= len(captures) {
				panic(fmt.Sprintf("invalid capture index %%%c", capture))
			}
			s.append(substitutionString(captures[index]))
		default:
			panic("invalid use of '%' in replacement string")
		}
	}
}

func substitutionString(value vm.Value) string {
	if value.IsString() {
		return value.AsString()
	}
	if value.IsNumber() {
		return value.String()
	}
	panic(fmt.Sprintf("string.gsub expected a string or number, got %s", value.Type()))
}

func (s *luaSubstitution) replace(replacement vm.Value, whole string, captures []vm.Value) bool {
	if len(captures) == 0 {
		captures = []vm.Value{vm.NewString(whole)}
	}
	if replacement.IsString() {
		start := s.output.Len()
		s.expand(replacement.AsString(), whole, captures)
		return s.output.String()[start:] != whole
	}
	var value vm.Value
	if replacement.IsTable() {
		var err error
		value, err = s.state.TableGet(replacement.AsTable(), captures[0])
		if err != nil {
			panic(err)
		}
	} else {
		results, err := s.state.ProtectedCall(replacement, captures)
		if err != nil {
			panic(err)
		}
		if len(results) > 0 {
			value = results[0]
		}
	}
	if value.IsNil() || (value.IsBool() && !value.AsBool()) {
		s.append(whole)
		return false
	}
	if !value.IsString() && !value.IsNumber() {
		panic(fmt.Sprintf("invalid replacement value (a %s)", value.Type()))
	}
	s.append(substitutionString(value))
	return true
}

func (s *luaSubstitution) run() int {
	defer s.state.EnterNonYieldable()()
	original := s.state.Get(1)
	input := substitutionString(original)
	pattern := substitutionString(s.state.Get(2))
	replacement := s.state.Get(3)
	if replacement.IsNumber() {
		replacement = vm.NewString(substitutionString(replacement))
	}
	if !replacement.IsString() && !replacement.IsTable() && !replacement.IsFunction() && !replacement.IsNativeFunc() {
		panic("string.gsub replacement must be a string, number, function or table")
	}
	maximum := int64(len(input)) + 1
	if s.state.ArgCount() >= 4 && !s.state.Get(4).IsNil() {
		value, ok := s.state.Get(4).ToInt()
		if !ok {
			panic("string.gsub replacement limit must be an integer")
		}
		maximum = max(0, value)
	}
	anchored := strings.HasPrefix(pattern, "^")
	position, lastEnd, count := 0, -1, int64(0)
	changed := false
	for position <= len(input) && count < maximum {
		if err := s.state.CheckInterrupt(); err != nil {
			panic(err)
		}
		arguments := []vm.Value{vm.NewString(input), vm.NewString(pattern), vm.NewInt(int64(position + 1))}
		match, err := s.state.ProtectedCall(s.find, arguments)
		if err != nil {
			panic(err)
		}
		if len(match) < 2 || match[0].IsNil() {
			break
		}
		start, end := int(match[0].AsInt())-1, int(match[1].AsInt())
		s.append(input[position:start])
		position = start
		if end != lastEnd {
			if s.replace(replacement, input[start:end], match[2:]) {
				changed = true
			}
			count++
			lastEnd = end
			position = end
		}
		// Lua suppresses an empty match immediately following another match.
		// Other empty matches copy one byte before searching again.
		if position == start {
			if position < len(input) {
				s.append(input[position : position+1])
			}
			position++
		}
		if anchored {
			break
		}
	}
	if position <= len(input) {
		s.append(input[position:])
	}
	if changed {
		s.state.Set(0, vm.NewString(s.output.String()))
	} else {
		// Preserve identity, including BSON-backed strings, on unchanged output.
		s.state.Set(0, original)
	}
	s.state.Set(1, vm.NewInt(count))
	return 2
}
