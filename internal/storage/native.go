package storage

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"
)

var ErrNativeUnsupported = errors.New("native operation is not supported")

// NativeStorage executes backend-native commands and managed cursor queries.
// Native mutations retain database semantics subject to adapter safeguards,
// including atomic revision maintenance for supported MongoDB writes.
type NativeStorage interface {
	Execute(context.Context, NativeRequest) (NativeResponse, error)
	Query(context.Context, QueryRequest) (QueryResponse, error)
	Count(context.Context, CountRequest) (CountResponse, error)
	Scan(context.Context, ScanRequest) (ScanResponse, error)
}

type NativeRequest struct {
	URI         string
	Method      string
	Path        string
	Query       string
	Headers     http.Header
	ContentType string
	Payload     []byte
	MaxBytes    int
}

type NativeResponse struct {
	ContentType string
	Payload     []byte
	Success     bool
	StatusCode  int
	Headers     http.Header
}

type ScanRequest struct {
	Emit       func(Document) error `json:"-"`
	Request    NativeRequest
	BatchSize  int
	Cursor     []byte
	Projection *Projection
}

type ScanResponse struct {
	Documents  []Document
	NextCursor []byte
}

type QueryRequest struct {
	Emit       func(Document) error `json:"-"`
	Request    NativeRequest
	Offset     int64
	PageSize   int
	Sort       []SortField
	Projection *Projection
}

type CountRequest struct {
	Request NativeRequest
}

type CountResponse struct {
	Count     uint64
	Estimated bool
}

type SortField struct {
	Field      string
	Descending bool
}

type Projection struct {
	Fields  []string
	Exclude bool
}

func (r QueryRequest) Validate() error {
	if r.Offset < 0 || r.PageSize < 1 || r.PageSize > 1000 {
		return InvalidArgumentError(errors.New("invalid query offset or page size"))
	}
	seen := make(map[string]bool)
	for _, field := range r.Sort {
		if strings.TrimSpace(field.Field) == "" || !utf8.ValidString(field.Field) || seen[field.Field] {
			return InvalidArgumentError(errors.New("sort fields must be nonempty, valid UTF-8 and unique"))
		}
		seen[field.Field] = true
	}
	return r.Projection.Validate()
}

func (p *Projection) Validate() error {
	if p == nil {
		return nil
	}
	seen := make(map[string]bool)
	for _, field := range p.Fields {
		if strings.TrimSpace(field) == "" || !utf8.ValidString(field) || seen[field] {
			return InvalidArgumentError(errors.New("projection fields must be nonempty, valid UTF-8 and unique"))
		}
		seen[field] = true
	}
	return nil
}

type QueryResponse struct {
	Documents []Document
	HasMore   bool
}
