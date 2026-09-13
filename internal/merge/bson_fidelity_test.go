package merge_test

import (
	"errors"
	"math"
	"testing"

	"github.com/liran/sink/internal/merge"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestLuaMergePreservesBSONScalarTypesAndLiteralObjects(t *testing.T) {
	decimal, err := bson.ParseDecimal128("123.45")
	if err != nil {
		t.Fatal(err)
	}
	scope := bson.D{{Key: "literal", Value: bson.D{{Key: "$numberInt", Value: "1"}}}, {Key: "long", Value: int64(1)}}
	values := []struct {
		name  string
		value any
	}{
		{name: "timestamp", value: bson.Timestamp{T: math.MaxUint32, I: 1}},
		{name: "min_key", value: bson.MinKey{}},
		{name: "max_key", value: bson.MaxKey{}},
		{name: "object_id", value: bson.NewObjectID()},
		{name: "binary", value: bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}},
		{name: "decimal", value: decimal},
		{name: "regex", value: bson.Regex{Pattern: "abc", Options: "im"}},
		{name: "javascript", value: bson.JavaScript("return 1")},
		{name: "code_with_scope", value: bson.CodeWithScope{Code: "return literal", Scope: scope}},
		{name: "symbol", value: bson.Symbol("name")},
		{name: "undefined", value: bson.Undefined{}},
		{name: "infinity", value: math.Inf(1)},
		{name: "date_outside_rfc3339", value: bson.DateTime(math.MaxInt64)},
		{name: "literal_integer", value: bson.D{{Key: "$numberInt", Value: "1"}}},
		{name: "literal_date", value: bson.D{{Key: "$date", Value: "2026-09-13T00:00:00Z"}}},
		{name: "literal_oid", value: bson.D{{Key: "$oid", Value: "000000000000000000000001"}}},
		{name: "literal_min_key", value: bson.D{{Key: "$minKey", Value: int32(1)}}},
	}
	for _, test := range values {
		t.Run(test.name, func(t *testing.T) {
			for _, source := range []string{
				`return function(current, incoming) return incoming end`,
				`return function(current, incoming) return {value = incoming.value, array = {incoming.value}} end`,
				`return function(current, incoming) current.value = incoming.value; return current end`,
			} {
				opts := merge.LuaOptions{}
				merger := compileTestProgram(t, []byte(source), opts)
				// Equal int32/int64 inputs must not make timestamp counters or
				// MinKey/MaxKey's structural integer markers ambiguous.
				fields := bson.D{{Key: "small", Value: int32(1)}, {Key: "wide", Value: int64(1)}, {Key: "value", Value: test.value}}
				incoming := bsonDocument(t, fields)
				empty := bson.D{}
				current := bsonDocument(t, empty)
				req := merge.Request{Current: &current, Incoming: incoming}
				result, err := merger.Merge(t.Context(), req)
				if err != nil {
					t.Fatal(err)
				}
				before := bson.Raw(incoming.Payload).Lookup("value")
				after := bson.Raw(result.Document.Payload).Lookup("value")
				if !before.Equal(after) {
					t.Fatalf("BSON value changed: %s %v -> %s %v", before.Type, before, after.Type, after)
				}
				if array, ok := bson.Raw(result.Document.Payload).Lookup("array").ArrayOK(); ok && !before.Equal(array.Index(0)) {
					t.Fatal("copying a BSON scalar into an array changed its type")
				}
			}
		})
	}
}

func TestLuaMergeKeepsNewAndTopLevelDollarFieldsAsDocuments(t *testing.T) {
	for _, source := range []string{
		`return function(current, incoming) return incoming end`,
		`return function(current, incoming) return {['$numberInt'] = '1'} end`,
	} {
		opts := merge.LuaOptions{}
		merger := compileTestProgram(t, []byte(source), opts)
		fields := bson.D{{Key: "$numberInt", Value: "1"}}
		incoming := bsonDocument(t, fields)
		req := merge.Request{Incoming: incoming}
		result, err := merger.Merge(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		value := bson.Raw(result.Document.Payload).Lookup("$numberInt")
		if value.Type != bson.TypeString || value.StringValue() != "1" {
			t.Fatalf("literal dollar field changed: %v", value)
		}
	}
}

func TestLuaMergeCanEditTypedBSONViewsWithoutRetypingBusinessObjects(t *testing.T) {
	opts := merge.LuaOptions{}
	source := []byte(`return function(current, incoming)
 incoming.timestamp['$timestamp'].i = incoming.timestamp['$timestamp'].i + 1
 incoming.code['$code'] = 'return updated'
 incoming.code['$scope'].long = incoming.code['$scope'].long + 1
 return incoming
end`)
	merger := compileTestProgram(t, source, opts)
	scope := bson.D{{Key: "literal", Value: bson.D{{Key: "$numberInt", Value: "1"}}}, {Key: "long", Value: int64(1)}}
	fields := bson.D{{Key: "timestamp", Value: bson.Timestamp{T: 123, I: 1}}, {Key: "code", Value: bson.CodeWithScope{Code: "return original", Scope: scope}}}
	req := merge.Request{Incoming: bsonDocument(t, fields)}
	result, err := merger.Merge(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	raw := bson.Raw(result.Document.Payload)
	seconds, increment := raw.Lookup("timestamp").Timestamp()
	code, updatedScope := raw.Lookup("code").CodeWithScope()
	if seconds != 123 || increment != 2 || code != "return updated" || updatedScope.Lookup("long").Int64() != 2 || updatedScope.Lookup("literal").Type != bson.TypeEmbeddedDocument {
		t.Fatal(raw)
	}
}

func TestLuaMergeRejectsDamagedTypedBSONViews(t *testing.T) {
	opts := merge.LuaOptions{}
	source := []byte(`return function(current, incoming) incoming.value['$timestamp'] = nil; return incoming end`)
	merger := compileTestProgram(t, source, opts)
	fields := bson.D{{Key: "value", Value: bson.Timestamp{T: 123, I: 1}}}
	req := merge.Request{Incoming: bsonDocument(t, fields)}
	if _, err := merger.Merge(t.Context(), req); !errors.Is(err, merge.ErrInvalidResult) {
		t.Fatalf("damaged typed value was accepted: %v", err)
	}
}

func TestLuaMergeRejectsAmbiguousOrDeepBSONBeforeConversion(t *testing.T) {
	opts := merge.LuaOptions{}
	source := []byte(`return function(current, incoming) return incoming end`)
	merger := compileTestProgram(t, source, opts)
	deep := bson.D{{Key: "value", Value: int32(1)}}
	for range 257 {
		deep = bson.D{{Key: "value", Value: deep}}
	}
	duplicate := bson.D{{Key: "value", Value: int32(1)}, {Key: "value", Value: int32(2)}}
	for _, fields := range []bson.D{deep, duplicate} {
		req := merge.Request{Incoming: bsonDocument(t, fields)}
		if _, err := merger.Merge(t.Context(), req); !errors.Is(err, merge.ErrInvalidIncoming) {
			t.Fatalf("unsafe BSON input was accepted: %v", err)
		}
	}
}
