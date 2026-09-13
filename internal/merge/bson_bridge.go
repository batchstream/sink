package merge

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// Only values identified by the BSON decoder carry a scalar type. A business
// document with Extended JSON field names remains an ordinary document.
type bsonScalar struct {
	kind bson.Type
	view map[string]any
}

func decodeBSONObject(raw bson.Raw, depth int) (map[string]any, error) {
	elements, err := raw.Elements()
	if err != nil {
		return nil, err
	}
	result := make(map[string]any, len(elements))
	for _, element := range elements {
		if _, exists := result[element.Key()]; exists {
			return nil, fmt.Errorf("BSON document contains duplicate field %q", element.Key())
		}
		value, err := decodeBSONValue(element.Value(), depth+1)
		if err != nil {
			return nil, err
		}
		result[element.Key()] = value
	}
	return result, nil
}

func decodeBSONValue(raw bson.RawValue, depth int) (any, error) {
	if depth > 256 {
		return nil, errors.New("BSON input exceeds the merge depth limit")
	}
	switch raw.Type {
	case bson.TypeEmbeddedDocument:
		return decodeBSONObject(raw.Document(), depth)
	case bson.TypeArray:
		values, err := raw.Array().Values()
		if err != nil {
			return nil, err
		}
		result := make([]any, len(values))
		for index, value := range values {
			result[index], err = decodeBSONValue(value, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case bson.TypeInt32, bson.TypeInt64:
		value := bsonInteger{value: raw.AsInt64(), wide: raw.Type == bson.TypeInt64}
		return value, nil
	case bson.TypeDouble:
		value := raw.Double()
		if !math.IsNaN(value) && !math.IsInf(value, 0) {
			return value, nil
		}
	case bson.TypeString:
		return raw.StringValue(), nil
	case bson.TypeBoolean:
		return raw.Boolean(), nil
	case bson.TypeNull:
		return nil, nil
	case bson.TypeDateTime:
		timestamp := time.UnixMilli(raw.DateTime()).UTC()
		// Dates outside RFC3339's year range retain a typed Extended JSON view.
		if timestamp.Year() >= 0 && timestamp.Year() <= 9999 {
			return bsonDateTime(timestamp.Format(time.RFC3339Nano)), nil
		}
	case bson.TypeCodeWithScope:
		code, scope := raw.CodeWithScope()
		decoded, err := decodeBSONObject(scope, depth+1)
		if err != nil {
			return nil, err
		}
		value := bsonScalar{kind: raw.Type, view: map[string]any{"$code": code, "$scope": decoded}}
		return value, nil
	}

	// Preserve the familiar Extended JSON view for BSON-only scalar values,
	// while keeping their type separate from the mutable Lua table contents.
	document := bson.D{{Key: "value", Value: raw}}
	encoded, err := bson.MarshalExtJSON(document, true, false)
	if err != nil {
		return nil, err
	}
	decoded, err := decodeJSONValue(encoded)
	if err != nil {
		return nil, err
	}
	view, ok := decoded.(map[string]any)["value"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("BSON type %s has no supported scalar view", raw.Type)
	}
	value := bsonScalar{kind: raw.Type, view: view}
	return value, nil
}

func encodeBSONScalar(kind bson.Type, decoded any) (any, error) {
	if kind == bson.TypeCodeWithScope {
		view, ok := decoded.(map[string]any)
		if !ok || len(view) != 2 {
			return nil, errors.New("BSON code with scope requires $code and $scope")
		}
		code, codeOK := view["$code"].(string)
		scope, scopeOK := view["$scope"].(map[string]any)
		if !codeOK || !scopeOK {
			return nil, errors.New("BSON code with scope requires a string and document")
		}
		value := bson.CodeWithScope{Code: bson.JavaScript(code), Scope: orderedBSONValue(scope)}
		return value, nil
	}
	wrapped := map[string]any{"value": decoded}
	encoded, err := json.Marshal(wrapped)
	if err != nil {
		return nil, err
	}
	var document bson.Raw
	if err := bson.UnmarshalExtJSON(encoded, false, &document); err != nil {
		return nil, fmt.Errorf("encode BSON %s: %w", kind, err)
	}
	value := document.Lookup("value")
	if value.Type != kind {
		return nil, fmt.Errorf("BSON scalar view changed type from %s to %s", kind, value.Type)
	}
	return value, nil
}

// Match the deterministic object ordering used by the JSON encoder. Native
// BSON values bypass Extended JSON so literal dollar-prefixed keys stay data.
func orderedBSONValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make(bson.D, 0, len(keys))
		for _, key := range keys {
			field := bson.E{Key: key, Value: orderedBSONValue(value[key])}
			result = append(result, field)
		}
		return result
	case []any:
		result := make(bson.A, len(value))
		for index, item := range value {
			result[index] = orderedBSONValue(item)
		}
		return result
	default:
		return value
	}
}
