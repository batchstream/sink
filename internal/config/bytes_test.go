package config

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestByteSizeDecoding(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{input: "123", want: 123},
		{input: "1_024", want: 1024},
		{input: "1B", want: 1},
		{input: "64KiB", want: 64 << 10},
		{input: "16MiB", want: 16 << 20},
		{input: "2GiB", want: 2 << 30},
		{input: "1TiB", want: 1 << 40},
		{input: "1KB", want: 1000},
		{input: "1MB", want: 1000000},
		{input: "1GB", want: 1000000000},
		{input: "1TB", want: 1000000000000},
		{input: "1.5MiB", want: 1572864},
		{input: "0.001KB", want: 1},
		{input: "'64 MiB'", want: 64 << 20},
		{input: fmt.Sprintf("%dB", math.MaxInt), want: math.MaxInt},
	}
	for _, test := range cases {
		t.Run(test.input, func(t *testing.T) {
			var value byteSize
			if err := yaml.Unmarshal([]byte(test.input), &value); err != nil {
				t.Fatal(err)
			}
			if int(value) != test.want {
				t.Fatalf("got %d bytes, want %d", value, test.want)
			}
		})
	}
}

func TestByteSizeRejectsAmbiguousLossyAndOverflowValues(t *testing.T) {
	for _, input := range []string{
		"16M", "16Mb", "16mib", "'16'", "16.0", "true", "[]", "{}",
		"1e3MB", "0.1B", "0.1KiB", "1/2MiB", "NaNMiB", "-1MiB",
		"9223372036854775808B", "8388608TiB", "9223372036854775808",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := Decode(strings.NewReader("mode: engine\ngrpc:\n  max_receive_message_bytes: "+input+"\n"), strings.NewReader(minimalStorage))
			if err == nil || !strings.Contains(err.Error(), "byte size") {
				t.Fatalf("expected actionable byte size error, got %v", err)
			}
		})
	}
}

func TestAllByteLimitsAcceptHumanReadableUnits(t *testing.T) {
	input := `mode: engine
memory: {max_bytes: 256MiB}
producer: {max_buffered_bytes: 64MiB}
batching: {max_bytes: 16MiB, queue: {max_bytes: 128MiB}}
execution:
 merge:
  lua: {max_source_bytes: 64KiB, max_result_bytes: 16MiB}
`
	human, err := Decode(strings.NewReader(input), strings.NewReader(kafkaStore))
	if err != nil {
		t.Fatal(err)
	}
	numeric := strings.NewReplacer("64MiB", "67108864", "16MiB", "16777216", "256MiB", "268435456", "128MiB", "134217728", "64KiB", "65536").Replace(input)
	bytes, err := Decode(strings.NewReader(numeric), strings.NewReader(kafkaStore))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(human, bytes) {
		t.Fatal("byte units changed configuration")
	}
}
