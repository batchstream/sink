package merge

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/iceisfun/golua/vm"
	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestLuaObjectConversionPreservesLargeObjects(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	bridge := newLuaJSONBridge(state)
	table := bridge.newObject(10000)
	expected := make(map[string]int, 10000)
	for index := range 10000 {
		key := "field" + strconv.Itoa(index)
		table.SetString(key, vm.NewInt(int64(index)))
		expected[key] = index
	}
	// Deleted entries and reinserted keys must not be emitted twice or lost.
	for index := 0; index < 10000; index += 3 {
		key := "field" + strconv.Itoa(index)
		table.SetString(key, vm.Nil)
		delete(expected, key)
		if index%2 == 0 {
			table.SetString(key, vm.NewInt(-1))
			expected[key] = -1
		}
	}
	value := vm.NewTable(table)
	if err := validateResultBudget(t.Context(), value, defaultMaxResultBytes); err != nil {
		t.Fatal(err)
	}
	for _, tagged := range []bool{true, false} {
		if !tagged {
			table.SetMetatable(nil)
		}
		count, array := inspectLuaTable(table)
		if array || count != len(expected) {
			t.Fatalf("inspect object: count=%d array=%v want %d fields", count, array, len(expected))
		}
		for _, encoding := range []storage.DocumentEncoding{storage.DocumentEncodingJSON, storage.DocumentEncodingBSON} {
			document, err := bridge.encodeJSONObject(value, encoding)
			if err != nil {
				t.Fatal(err)
			}
			if err := storage.ValidateDocument(document); err != nil {
				t.Fatal(err)
			}
			var decoded map[string]int
			if encoding == storage.DocumentEncodingJSON {
				err = json.Unmarshal(document.Payload, &decoded)
			} else {
				err = bson.Unmarshal(document.Payload, &decoded)
			}
			if err != nil || !reflect.DeepEqual(decoded, expected) {
				t.Fatalf("object fields changed: tagged=%v encoding=%d error=%v", tagged, encoding, err)
			}
		}
	}
}

func TestLuaResultTraversalRejectsCyclesAndCancellation(t *testing.T) {
	state := vm.New()
	defer state.Close(context.Background())
	bridge := newLuaJSONBridge(state)
	table := bridge.newObject(1)
	value := vm.NewTable(table)
	table.SetString("self", value)
	if err := validateResultBudget(t.Context(), value, defaultMaxResultBytes); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("cycle error = %v", err)
	}
	table.SetString("self", vm.Nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := validateResultBudget(ctx, value, defaultMaxResultBytes); !errors.Is(err, ErrExecutionDeadline) {
		t.Fatalf("cancellation error = %v", err)
	}
}

func BenchmarkLuaObjectConversion(b *testing.B) {
	for _, fields := range []int{1000, 10000, 30000} {
		b.Run(strconv.Itoa(fields), func(b *testing.B) {
			state := vm.New()
			defer state.Close(context.Background())
			bridge := newLuaJSONBridge(state)
			table := bridge.newObject(fields)
			for index := range fields {
				table.SetString("field"+strconv.Itoa(index), vm.NewInt(int64(index)))
			}
			value := vm.NewTable(table)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := validateResultBudget(b.Context(), value, defaultMaxResultBytes); err != nil {
					b.Fatal(err)
				}
				if _, err := bridge.encodeJSONObject(value, storage.DocumentEncodingJSON); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
