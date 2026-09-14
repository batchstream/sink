package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

func CloneDocument(document Document) Document {
	cloned := Document{
		Encoding: document.Encoding,
		Payload:  bytes.Clone(document.Payload),
	}
	return cloned
}

func ValidateDocument(document Document) error {
	switch document.Encoding {
	case DocumentEncodingJSON:
		trimmed := bytes.TrimSpace(document.Payload)
		if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' || !utf8.Valid(document.Payload) || !json.Valid(document.Payload) {
			return errors.New("document payload must contain a valid UTF-8 JSON object")
		}
	case DocumentEncodingBSON:
		if err := ValidateBSONDocument(document.Payload); err != nil {
			return fmt.Errorf("document payload must contain a valid BSON document: %w", err)
		}
	default:
		return errors.New("document encoding is required")
	}
	return nil
}
