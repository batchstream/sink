package merge

import (
	"strconv"
	"strings"

	"github.com/iceisfun/golua/vm"
)

// Render one directive at a time, checking expanding strings before calling
// the runtime and checking every append before growing the combined output.
func boundedLuaFormat(state *vm.VM, formatter vm.Value, maximum int) int {
	arguments := luaArguments(state)
	if len(arguments) == 0 {
		panic("string.format requires a format")
	}
	format, err := luaPackString(arguments[0])
	if err != nil {
		panic("string.format requires a string or number format")
	}
	var output strings.Builder
	appendPart := func(part string) {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		if len(part) > maximum-output.Len() {
			panic(nativeAllocationLimit)
		}
		output.WriteString(part)
	}
	argument := 1
	for len(format) > 0 {
		index := strings.IndexByte(format, '%')
		if index < 0 {
			appendPart(format)
			break
		}
		appendPart(format[:index])
		format = format[index:]
		if strings.HasPrefix(format, "%%") {
			appendPart("%")
			format = format[2:]
			continue
		}
		end := 1
		for end < len(format) && strings.ContainsRune("#0- +.0123456789", rune(format[end])) {
			end++
			// Lua permits at most 20 modifier bytes. Bound parsing too, before
			// the runtime repeatedly concatenates an invalid long specification.
			if end > 21 {
				panic("invalid format (too long)")
			}
		}
		if end < len(format) {
			end++
		}
		spec := format[:end]
		format = format[end:]
		values := []vm.Value{vm.NewString(spec)}
		if argument < len(arguments) {
			value := arguments[argument]
			if value.IsString() {
				check := formatStringCheck{formatter: formatter, spec: spec, value: value.AsString(), maximum: maximum - output.Len()}
				checkFormattedString(state, check)
			}
			values = append(values, value)
		}
		argument++
		result, err := state.ProtectedCall(formatter, values)
		if err != nil {
			panic(err)
		}
		appendPart(result[0].AsString())
	}
	state.Set(0, vm.NewString(output.String()))
	return 1
}

type formatStringCheck struct {
	formatter vm.Value
	spec      string
	value     string
	maximum   int
}

func checkFormattedString(state *vm.VM, check formatStringCheck) {
	size := len(check.value)
	switch {
	case check.spec == "%q":
		size = 2 // Opening and closing quotes.
		for index := range len(check.value) {
			if index%1024 == 0 {
				if err := state.CheckInterrupt(); err != nil {
					panic(err)
				}
			}
			value := check.value[index]
			switch {
			case value == '"' || value == '\\' || value == '\n':
				size += 2
			case value < 32 || value == 127:
				// Lua uses decimal escapes, padded to three digits before a digit.
				if index+1 < len(check.value) && check.value[index+1] >= '0' && check.value[index+1] <= '9' {
					size += 4
				} else {
					size += 1 + len(strconv.Itoa(int(value)))
				}
			default:
				size++
			}
			if size > check.maximum {
				panic(nativeAllocationLimit)
			}
		}
	case check.spec == "%s":
	case strings.HasSuffix(check.spec, "s"):
		// Modified %s rejects embedded NULs before allocating. Let the runtime
		// report that error; otherwise validate modifiers using an empty value.
		if strings.IndexByte(check.value, 0) >= 0 {
			return
		}
		arguments := []vm.Value{vm.NewString(check.spec), vm.NewString("")}
		padded, err := state.ProtectedCall(check.formatter, arguments)
		if err != nil {
			panic(err)
		}
		if dot := strings.IndexByte(check.spec, '.'); dot >= 0 {
			precision, _ := strconv.Atoi(check.spec[dot+1 : len(check.spec)-1])
			size = min(size, precision)
		}
		size = max(size, len(padded[0].AsString()))
	default:
		// Numeric conversions have bounded widths/precision in the runtime.
		return
	}
	if size > check.maximum {
		panic(nativeAllocationLimit)
	}
}
