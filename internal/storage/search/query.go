package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/liran/sink/internal/storage"
)

func (s *Store) pageOptions(req storage.NativeRequest) (requestOptions, map[string]json.RawMessage, error) {
	if req.Path == "" {
		req.Path = "/_search"
	}
	if req.Method == "" {
		req.Method = http.MethodPost
	}
	opts, err := s.nativeOptions(req)
	if err != nil {
		return opts, nil, err
	}
	index, endpoint, _ := strings.Cut(strings.TrimPrefix(opts.path, "/"), "/")
	indexed := index != "" && (!strings.HasPrefix(index, "_") || index == "_all" || strings.ContainsAny(index, ",:*?"))
	searchPath := opts.path == "/_search" || (indexed && endpoint == "_search")
	// A document ID or administrative resource can also end in /_search.
	// Reject encoded separators so decoding cannot change the route shape.
	if !searchPath || strings.Count(opts.path, "/") != strings.Count(opts.rawPath, "/") ||
		(opts.method != http.MethodGet && opts.method != http.MethodPost) {
		return opts, nil, errors.New("Query, Count and Scan require /_search or /{index}/_search")
	}
	mediaType, _, _ := mime.ParseMediaType(opts.contentType)
	if opts.contentType != "" && mediaType != ContentTypeJSON && !strings.HasSuffix(mediaType, "+json") {
		return opts, nil, errors.New("Query and Count require a JSON content_type")
	}
	for _, name := range []string{"source", "filter_path", "scroll", "scroll_id", "search_after", "pit"} {
		if opts.query.Has(name) {
			return opts, nil, fmt.Errorf("Query and Count do not support parameter %q", name)
		}
	}
	body := make(map[string]json.RawMessage)
	if len(opts.payload) > 0 {
		if !utf8.Valid(opts.payload) {
			return opts, nil, errors.New("Query, Count and Scan require a valid UTF-8 JSON body")
		}
		if err := json.Unmarshal(opts.payload, &body); err != nil || body == nil {
			return opts, nil, errors.New("Query and Count require a JSON object body")
		}
	}
	for _, name := range []string{"search_after", "pit"} {
		if _, exists := body[name]; exists {
			return opts, nil, fmt.Errorf("Query and Count do not support %q; use Execute for manual pagination", name)
		}
	}
	delete(body, "from")
	delete(body, "size")
	opts.query.Del("from")
	opts.query.Del("size")
	opts.method = http.MethodPost
	// Managed pages never open backend cursors and can safely retry the same
	// query on another endpoint after a transport or temporary HTTP failure.
	opts.retrySafe = true
	opts.contentType = ContentTypeJSON
	opts.headers.Set("Accept", ContentTypeJSON)
	return opts, body, nil
}

func (s *Store) performQuery(ctx context.Context, opts requestOptions) (scanPage, error) {
	var page scanPage
	if opts.emitHit != nil {
		opts.decode = func(reader io.Reader) error {
			decoded, err := decodeSearchPage(reader, opts.emitHit)
			page = decoded
			return err
		}
	}
	response, err := s.perform(ctx, opts)
	if err != nil {
		if errors.Is(err, errResponseTooLarge) {
			return page, storage.ResourceExhaustedError(err)
		}
		return page, err
	}
	defer response.close()
	if response.statusCode < 200 || response.statusCode >= 300 {
		return page, responseError(s.driver, response)
	}
	if opts.decode == nil {
		if err := json.Unmarshal(response.body, &page); err != nil {
			return page, fmt.Errorf("decode search query: %w", err)
		}
	}
	if page.ScrollID != "" || page.Hits == nil || page.Hits.Hits == nil || page.TimedOut == nil || *page.TimedOut || page.TerminatedEarly {
		return page, errors.New("search query returned incomplete results, failed shards or an unexpected cursor")
	}
	shards := page.Shards
	if shards == nil || shards.Total == nil || shards.Successful == nil || shards.Failed == nil ||
		*shards.Total < 0 || *shards.Successful != *shards.Total || *shards.Failed != 0 {
		return page, errors.New("search query returned incomplete results, failed shards or an unexpected cursor")
	}
	// Cross-cluster search can return HTTP 200 with every reported shard
	// successful while omitting an unavailable remote cluster entirely.
	if clusters := page.Clusters; clusters != nil {
		if clusters.Total == nil || clusters.Successful == nil || clusters.Skipped == nil ||
			*clusters.Total < 0 || *clusters.Successful != *clusters.Total || *clusters.Skipped != 0 ||
			clusters.Running != 0 || clusters.Partial != 0 || clusters.Failed != 0 {
			return page, errors.New("search query returned incomplete cluster results")
		}
	}
	return page, nil
}

func (s *Store) Query(ctx context.Context, req storage.QueryRequest) (storage.QueryResponse, error) {
	var empty storage.QueryResponse
	if err := req.Validate(); err != nil {
		return empty, err
	}
	opts, body, err := s.pageOptions(req.Request)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	if _, exists := body["collapse"]; exists {
		return empty, storage.InvalidArgumentError(errors.New("Query cannot determine has_more for collapsed hits; use Execute for native collapsed pagination"))
	}
	if req.Offset > math.MaxInt64-int64(req.PageSize) {
		return empty, storage.InvalidArgumentError(errors.New("query page end exceeds the supported offset range"))
	}
	end := req.Offset + int64(req.PageSize)
	body["from"] = json.RawMessage(fmt.Sprint(req.Offset))
	body["size"] = json.RawMessage(fmt.Sprint(req.PageSize))
	// Count only far enough to prove another hit exists. Fetching an extra
	// document consumes its source budget and exceeds the final legal window.
	body["track_total_hits"] = json.RawMessage("true")
	if end < math.MaxInt32 {
		body["track_total_hits"] = json.RawMessage(fmt.Sprint(end + 1))
	}
	opts.query.Del("track_total_hits")
	opts.query.Set("rest_total_hits_as_int", "false")
	if len(req.Sort) > 0 {
		sort := make([]map[string]string, 0, len(req.Sort))
		for _, field := range req.Sort {
			direction := "asc"
			if field.Descending {
				direction = "desc"
			}
			item := map[string]string{field.Field: direction}
			sort = append(sort, item)
		}
		body["sort"], err = json.Marshal(sort)
		if err != nil {
			return empty, storage.InvalidArgumentError(err)
		}
		opts.query.Del("sort")
	}
	if err := applyProjection(&opts, body, req.Projection); err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	opts.payload, err = json.Marshal(body)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	emitted := 0
	if req.Emit != nil {
		opts.emitHit = func(hit json.RawMessage) error {
			emitted++
			if emitted > req.PageSize {
				return errors.New("search query exceeded its requested result count")
			}
			if len(hit) > req.Request.MaxBytes && req.Request.MaxBytes > 0 {
				return storage.ResourceExhaustedError(errResponseTooLarge)
			}
			document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: hit}
			return req.Emit(document)
		}
	}
	page, err := s.performQuery(ctx, opts)
	if err != nil {
		return empty, err
	}
	if len(page.Hits.Hits) > req.PageSize {
		return empty, errors.New("search query exceeded its requested result count")
	}
	hasMore, err := queryHasMore(page.Hits, req)
	if err != nil {
		return empty, err
	}
	result := storage.QueryResponse{HasMore: hasMore}
	if req.Emit != nil {
		return result, nil
	}
	budget := storage.NewReadBudget(req.Request.MaxBytes)
	for _, hit := range page.Hits.Hits {
		if err := budget.Reserve(len(hit)); err != nil {
			return empty, err
		}
		document := storage.Document{Encoding: storage.DocumentEncodingJSON, Payload: hit}
		result.Documents = append(result.Documents, document)
	}
	return result, nil
}

func queryHasMore(hits *scanHits, req storage.QueryRequest) (bool, error) {
	var total struct {
		Value    *int64 `json:"value"`
		Relation string `json:"relation"`
	}
	err := json.Unmarshal(hits.Total, &total)
	if err != nil || total.Value == nil || *total.Value < 0 {
		return false, errors.New("search query omitted a nonnegative total")
	}
	end := req.Offset + int64(req.PageSize)
	switch total.Relation {
	case "eq":
		expected := min(int64(req.PageSize), max(0, *total.Value-req.Offset))
		if int64(len(hits.Hits)) != expected {
			return false, errors.New("search query hit count does not match its exact total")
		}
		return *total.Value > end, nil
	case "gte":
		if *total.Value > end && len(hits.Hits) == req.PageSize {
			return true, nil
		}
	}
	return false, errors.New("search query total cannot prove has_more")
}

func (s *Store) Count(ctx context.Context, req storage.CountRequest) (storage.CountResponse, error) {
	var empty storage.CountResponse
	opts, body, err := s.pageOptions(req.Request)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	// Count needs matching-document totals, without computing hit presentation,
	// aggregations or collapsing the hits returned by a normal search.
	for _, name := range []string{"sort", "_source", "fields", "docvalue_fields", "stored_fields", "highlight", "script_fields", "aggs", "aggregations", "collapse", "rescore", "suggest", "profile"} {
		delete(body, name)
	}
	for _, name := range []string{"sort", "_source", "_source_includes", "_source_excludes", "stored_fields", "docvalue_fields"} {
		opts.query.Del(name)
	}
	body["size"] = json.RawMessage("0")
	body["track_total_hits"] = json.RawMessage("true")
	opts.query.Del("track_total_hits")
	opts.query.Set("rest_total_hits_as_int", "false")
	opts.payload, err = json.Marshal(body)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	page, err := s.performQuery(ctx, opts)
	if err != nil {
		return empty, err
	}
	var total struct {
		Value    *int64 `json:"value"`
		Relation string `json:"relation"`
	}
	err = json.Unmarshal(page.Hits.Total, &total)
	if err != nil || total.Value == nil || *total.Value < 0 || total.Relation != "eq" || len(page.Hits.Hits) != 0 {
		return empty, errors.New("search count omitted an exact nonnegative total")
	}
	result := storage.CountResponse{Count: uint64(*total.Value)}
	return result, nil
}

// applyProjection shares source selection and query-parameter precedence between
// Query and Scan. Hit metadata, including continuation sort values, is preserved.
func applyProjection(opts *requestOptions, body map[string]json.RawMessage, p *storage.Projection) error {
	if p == nil {
		return nil
	}
	body["_source"] = json.RawMessage("true")
	if len(p.Fields) > 0 {
		mode := "includes"
		if p.Exclude {
			mode = "excludes"
		}
		projection := map[string][]string{mode: p.Fields}
		encoded, err := json.Marshal(projection)
		if err != nil {
			return err
		}
		body["_source"] = encoded
	}
	for _, name := range []string{"_source", "_source_includes", "_source_excludes"} {
		opts.query.Del(name)
	}
	return nil
}
