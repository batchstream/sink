package main

import (
	"encoding/json"
	"math/big"
	"strings"
)

// Keep numbers distinct from strings and compare their exact decimal values.
// Floating-point conversion would hide differences between large integers.
type luaTestNumber struct {
	coefficient string
	exponent    string
}

func normalizeLuaTestNumbers(value any) any {
	switch typed := value.(type) {
	case json.Number:
		return normalizeLuaTestNumber(typed)
	case map[string]any:
		for key, member := range typed {
			typed[key] = normalizeLuaTestNumbers(member)
		}
	case []any:
		for index, member := range typed {
			typed[index] = normalizeLuaTestNumbers(member)
		}
	}
	return value
}

func normalizeLuaTestNumber(value json.Number) any {
	mantissa := string(value)
	var exponent big.Int
	if index := strings.IndexAny(mantissa, "eE"); index >= 0 {
		if _, ok := exponent.SetString(mantissa[index+1:], 10); !ok {
			return value
		}
		mantissa = mantissa[:index]
	}
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(mantissa, "-")
	fractionalDigits := 0
	if index := strings.IndexByte(mantissa, '.'); index >= 0 {
		fractionalDigits = len(mantissa) - index - 1
		mantissa = mantissa[:index] + mantissa[index+1:]
	}
	mantissa = strings.TrimLeft(mantissa, "0")
	if mantissa == "" {
		zero := luaTestNumber{coefficient: "0", exponent: "0"}
		return zero
	}
	coefficient := strings.TrimRight(mantissa, "0")
	shift := len(mantissa) - len(coefficient) - fractionalDigits
	// Store the exponent itself; expanding powers of ten would let a short
	// expected fixture such as 1e1000000000 allocate unbounded memory.
	exponent.Add(&exponent, big.NewInt(int64(shift)))
	if negative {
		coefficient = "-" + coefficient
	}
	number := luaTestNumber{coefficient: coefficient, exponent: exponent.String()}
	return number
}
