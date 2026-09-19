package storage

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestValidateJSONDocumentUTF8AndWhitespace(t *testing.T) {
	for _, payload := range []string{"{\"value\":\"\xff\"}", "{\"\xff\":1,\"\xfe\":2}", "{\"value\":\"\xc0\xaf\"}", "\u00a0{}\u00a0", "\v{}\v"} {
		document := Document{Encoding: DocumentEncodingJSON, Payload: []byte(payload)}
		if err := ValidateDocument(document); err == nil {
			t.Errorf("malformed JSON was accepted: %q", payload)
		}
	}
	for _, payload := range []string{" \t\r\n{\"中文😀\":\"�\"}\t\r\n ", `{"value":"\ud83d\ude00"}`} {
		document := Document{Encoding: DocumentEncodingJSON, Payload: []byte(payload)}
		if err := ValidateDocument(document); err != nil {
			t.Errorf("valid Unicode JSON rejected: %v", err)
		}
	}
}

func bsonEnvelope(kind bson.Type, payload []byte) []byte {
	encoded := make([]byte, 0, len(payload)+8)
	encoded = append(encoded, 0, 0, 0, 0, byte(kind), 'v', 0)
	encoded = append(encoded, payload...)
	encoded = append(encoded, 0)
	binary.LittleEndian.PutUint32(encoded, uint32(len(encoded)))
	return encoded
}

func TestValidateDocumentRejectsMalformedNestedBSON(t *testing.T) {
	invalid := []byte{7, 0, 0, 0, 0x42, 0, 0}
	deep := []byte{5, 0, 0, 0, 0}
	for range 300 {
		deep = bsonEnvelope(bson.TypeEmbeddedDocument, deep)
	}
	arrayDocument := bson.D{{Key: "1", Value: int32(2)}}
	array, err := bson.Marshal(arrayDocument)
	if err != nil {
		t.Fatal(err)
	}
	codeScope := []byte{16, 0, 0, 0, 1, 0, 0, 0, 0}
	codeScope = append(codeScope, invalid...)
	cases := map[string][]byte{
		"trailing bytes":       {5, 0, 0, 0, 0, 1},
		"embedded document":    bsonEnvelope(bson.TypeEmbeddedDocument, invalid),
		"embedded array":       bsonEnvelope(bson.TypeArray, invalid),
		"array indexes":        bsonEnvelope(bson.TypeArray, array),
		"nesting limit":        deep,
		"empty string framing": bsonEnvelope(bson.TypeString, []byte{0, 0, 0, 0}),
		"string terminator":    bsonEnvelope(bson.TypeString, []byte{2, 0, 0, 0, 'a', 'b'}),
		"string UTF-8":         bsonEnvelope(bson.TypeString, []byte{2, 0, 0, 0, 0xff, 0}),
		"invalid boolean":      bsonEnvelope(bson.TypeBoolean, []byte{2}),
		"binary subtype 2":     bsonEnvelope(bson.TypeBinary, []byte{4, 0, 0, 0, 2, 1, 0, 0, 0}),
		"code scope":           bsonEnvelope(bson.TypeCodeWithScope, codeScope),
	}
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			document := Document{Encoding: DocumentEncodingBSON, Payload: payload}
			if err := ValidateDocument(document); err == nil {
				t.Fatal("malformed BSON accepted as a valid document")
			}
		})
	}
}

func TestValidateBSONDepthBoundary(t *testing.T) {
	payload := []byte{5, 0, 0, 0, 0}
	for range maxBSONDepth {
		payload = bsonEnvelope(bson.TypeEmbeddedDocument, payload)
	}
	if err := ValidateBSONDocument(payload); err != nil {
		t.Fatalf("document at depth limit rejected: %v", err)
	}
	payload = bsonEnvelope(bson.TypeEmbeddedDocument, payload)
	if err := ValidateBSONDocument(payload); err == nil {
		t.Fatal("document beyond depth limit accepted")
	}
}

func TestValidateBSONBoundsMalformedArrayDiagnostic(t *testing.T) {
	array := bson.D{{Key: strings.Repeat("x", 64<<10), Value: int32(1)}}
	payload, err := bson.Marshal(array)
	if err != nil {
		t.Fatal(err)
	}
	err = ValidateBSONDocument(bsonEnvelope(bson.TypeArray, payload))
	if err == nil || len(err.Error()) > 256 {
		t.Fatal("malformed BSON array diagnostic must be nonempty and bounded")
	}
}

func TestValidateBSONPreservesNativeTypes(t *testing.T) {
	scope := bson.D{{Key: "nested", Value: bson.A{int32(1), "value"}}}
	code := bson.CodeWithScope{Code: "return nested", Scope: scope}
	value := bson.D{
		{Key: "object", Value: scope}, {Key: "code", Value: code},
		{Key: "binary", Value: bson.Binary{Subtype: 2, Data: []byte{0, 1, 2}}},
		{Key: "date", Value: bson.DateTime(123)}, {Key: "id", Value: bson.NewObjectID()},
		{Key: "regex", Value: bson.Regex{Pattern: "a+", Options: "i"}},
		{Key: "pointer", Value: bson.DBPointer{DB: "collection", Pointer: bson.NewObjectID()}},
		{Key: "null", Value: nil}, {Key: "bool", Value: true},
		{Key: "string", Value: "你好\x00world"}, {Key: "symbol", Value: bson.Symbol("value")},
		{Key: "duplicate", Value: int32(1)}, {Key: "duplicate", Value: int64(2)},
	}
	payload, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	original := bytes.Clone(payload)
	if err := ValidateBSONDocument(payload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(payload, original) {
		t.Fatal("validation changed the document")
	}
	for _, suffix := range []byte{0, 1} {
		malformed := append(bytes.Clone(payload), suffix)
		if err := ValidateBSONDocument(malformed); err == nil {
			t.Fatal("trailing content accepted")
		}
	}
}
