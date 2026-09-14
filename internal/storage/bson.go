package storage

import (
	"errors"
	"fmt"
	"strconv"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

const maxBSONDepth = 256

// ValidateBSONDocument requires exactly one complete document and validates
// nested documents, arrays and code scopes before any recursive decoding.
// bson.Raw.Validate checks only the outer document's element framing.
func ValidateBSONDocument(payload []byte) error {
	return validateBSONContainer(payload, 0, false)
}

func validateBSONContainer(payload []byte, depth int, array bool) error {
	if depth > maxBSONDepth {
		return errors.New("BSON nesting exceeds 256")
	}
	document, trailing, ok := bsoncore.ReadDocument(payload)
	if !ok || len(trailing) != 0 || len(document) < 5 || document[len(document)-1] != 0 {
		return errors.New("invalid BSON document length or terminator")
	}
	remaining := document[4 : len(document)-1]
	for index := 0; len(remaining) > 0; index++ {
		element, rest, ok := bsoncore.ReadElement(remaining)
		if !ok {
			return errors.New("invalid BSON element")
		}
		if !utf8.Valid(element.KeyBytes()) {
			return errors.New("BSON field name contains invalid UTF-8")
		}
		if array && element.Key() != strconv.Itoa(index) {
			return fmt.Errorf("BSON array key %q is out of order or invalid", element.Key())
		}
		if err := validateBSONValue(element.Value(), depth); err != nil {
			return err
		}
		remaining = rest
	}
	return nil
}

func validateBSONValue(value bsoncore.Value, depth int) error {
	switch value.Type {
	case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
		return validateBSONContainer(value.Data, depth+1, value.Type == bsoncore.TypeArray)
	case bsoncore.TypeString, bsoncore.TypeJavaScript, bsoncore.TypeSymbol:
		remaining, err := validateBSONString(value.Data)
		if err != nil || len(remaining) != 0 {
			return errors.New("invalid BSON string")
		}
	case bsoncore.TypeCodeWithScope:
		if len(value.Data) < 4 {
			return errors.New("invalid BSON code with scope")
		}
		scope, err := validateBSONString(value.Data[4:])
		if err != nil {
			return err
		}
		return validateBSONContainer(scope, depth+1, false)
	case bsoncore.TypeBoolean:
		if value.Data[0] > 1 {
			return errors.New("invalid BSON boolean")
		}
	case bsoncore.TypeBinary:
		_, _, remaining, ok := bsoncore.ReadBinary(value.Data)
		if !ok || len(remaining) != 0 {
			return errors.New("invalid BSON binary length")
		}
	case bsoncore.TypeDBPointer:
		remaining, err := validateBSONString(value.Data)
		if err != nil || len(remaining) != 12 {
			return errors.New("invalid BSON DBPointer")
		}
	case bsoncore.TypeRegex:
		if !utf8.Valid(value.Data) {
			return errors.New("BSON regular expression contains invalid UTF-8")
		}
	}
	return nil
}

func validateBSONString(payload []byte) ([]byte, error) {
	length, remaining, ok := bsoncore.ReadLength(payload)
	if !ok || length < 1 || int(length) > len(remaining) || remaining[length-1] != 0 || !utf8.Valid(remaining[:length-1]) {
		return nil, errors.New("invalid BSON string length, terminator or UTF-8")
	}
	return remaining[length:], nil
}
