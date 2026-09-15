package config

import (
	"fmt"
	"math/big"
	"regexp"

	"gopkg.in/yaml.v3"
)

// byteSize exists only at the YAML boundary. Runtime limits remain byte counts.
type byteSize int

var byteSizePattern = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)[ \t]*(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)$`)

func (size *byteSize) UnmarshalYAML(node *yaml.Node) error {
	invalid := fmt.Errorf("line %d: byte size must be an integer byte count or a size such as 16MiB; use B, KB, MB, GB, TB, KiB, MiB, GiB, or TiB with an exact whole-byte result that fits an integer", node.Line)
	if node.Kind != yaml.ScalarNode {
		return invalid
	}
	if node.Tag == "!!int" {
		var value int
		if err := node.Decode(&value); err != nil {
			return invalid
		}
		*size = byteSize(value)
		return nil
	}
	if node.Tag != "!!str" {
		return invalid
	}
	parts := byteSizePattern.FindStringSubmatch(node.Value)
	if parts == nil {
		return invalid
	}
	multipliers := map[string]int64{
		"B": 1, "KB": 1_000, "MB": 1_000_000, "GB": 1_000_000_000, "TB": 1_000_000_000_000,
		"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40,
	}
	amount, ok := new(big.Rat).SetString(parts[1])
	if !ok {
		return invalid
	}
	amount.Mul(amount, new(big.Rat).SetInt64(multipliers[parts[2]]))
	if !amount.IsInt() || !amount.Num().IsInt64() {
		return invalid
	}
	value := amount.Num().Int64()
	if int64(int(value)) != value {
		return invalid
	}
	*size = byteSize(value)
	return nil
}

func (v *validator) bytes(name string, value *byteSize, fallback int, maximum int) int {
	return v.bounded(name, (*int)(value), fallback, maximum)
}
