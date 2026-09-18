package search

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type bulkItem struct {
	Index       string       `json:"_index"`
	ID          string       `json:"_id"`
	Status      int          `json:"status"`
	Sequence    *int64       `json:"_seq_no"`
	PrimaryTerm *int64       `json:"_primary_term"`
	Error       *errorDetail `json:"error"`
}

type bulkResponse struct {
	Errors bool                  `json:"errors"`
	Items  []map[string]bulkItem `json:"items"`
}

type bulkAction struct {
	name string
	id   string
}

type bulkRequest struct {
	payload          []byte
	actions          []bulkAction
	waitUntilVisible bool
}

func (s *Store) performBulk(ctx context.Context, request bulkRequest) ([]bulkItem, error) {
	query := make(url.Values)
	if request.waitUntilVisible {
		query.Set("refresh", "wait_for")
	}
	opts := requestOptions{
		method:      http.MethodPost,
		path:        "/_bulk",
		contentType: "application/x-ndjson",
		payload:     request.payload,
		query:       query,
	}
	response, err := s.perform(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("execute search bulk request: %w", err)
	}
	defer response.close()
	if response.statusCode < 200 || response.statusCode >= 300 {
		return nil, responseError(s.driver, response)
	}
	var decoded bulkResponse
	if err := json.Unmarshal(response.body, &decoded); err != nil {
		return nil, fmt.Errorf("decode search bulk response: %w", err)
	}
	if len(decoded.Items) != len(request.actions) {
		return nil, fmt.Errorf("search returned %d bulk results for %d operations", len(decoded.Items), len(request.actions))
	}
	items := make([]bulkItem, 0, len(decoded.Items))
	for index, action := range decoded.Items {
		if len(action) != 1 {
			return nil, fmt.Errorf("search bulk result %d contains %d actions", index, len(action))
		}
		expected := request.actions[index]
		item, exists := action[expected.name]
		// Concrete index names can differ from requested aliases. A response
		// must still identify its index, document and exact mutation kind.
		if !exists || item.Index == "" || item.ID != expected.id {
			return nil, fmt.Errorf("search bulk result %d has a mismatched action or document ID, or a missing index", index)
		}
		items = append(items, item)
	}
	return items, nil
}
