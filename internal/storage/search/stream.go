package search

import (
	"encoding/json"
	"errors"
	"io"
)

// Decode one hit at a time. Metadata can appear after hits in a JSON response;
// callers must wait for validation and stream completion before checkpointing.
func decodeSearchPage(reader io.Reader, emit func(json.RawMessage) error) (scanPage, error) {
	var page scanPage
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	err := objectFields(decoder, func(field string) error {
		switch field {
		case "_scroll_id":
			return decoder.Decode(&page.ScrollID)
		case "timed_out":
			return decoder.Decode(&page.TimedOut)
		case "terminated_early":
			return decoder.Decode(&page.TerminatedEarly)
		case "_shards":
			return decoder.Decode(&page.Shards)
		case "_clusters":
			return decoder.Decode(&page.Clusters)
		case "hits":
			page.Hits = &scanHits{}
			return objectFields(decoder, func(field string) error {
				switch field {
				case "total":
					return decoder.Decode(&page.Hits.Total)
				case "hits":
					if err := delimiter(decoder, '['); err != nil {
						return err
					}
					page.Hits.Hits = make([]json.RawMessage, 0)
					for decoder.More() {
						var hit json.RawMessage
						if err := decoder.Decode(&hit); err != nil {
							return err
						}
						if err := emit(hit); err != nil {
							return err
						}
						// Keep only the count required by total/has_more validation.
						page.Hits.Hits = append(page.Hits.Hits, nil)
					}
					return delimiter(decoder, ']')
				default:
					return discardJSON(decoder)
				}
			})
		default:
			return discardJSON(decoder)
		}
	})
	if err == nil {
		err = jsonEnd(decoder)
	}
	return page, err
}

func objectFields(decoder *json.Decoder, visit func(string) error) error {
	if err := delimiter(decoder, '{'); err != nil {
		return err
	}
	seen := make(map[string]bool)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		field, ok := token.(string)
		if !ok || seen[field] {
			return errors.New("invalid or duplicate JSON field")
		}
		seen[field] = true
		if err := visit(field); err != nil {
			return err
		}
	}
	return delimiter(decoder, '}')
}
func delimiter(decoder *json.Decoder, expected json.Delim) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if token != expected {
		return errors.New("unexpected JSON delimiter")
	}
	return nil
}
func discardJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch token {
	case json.Delim('{'):
		for decoder.More() {
			if _, err := decoder.Token(); err != nil {
				return err
			}
			if err := discardJSON(decoder); err != nil {
				return err
			}
		}
		return delimiter(decoder, '}')
	case json.Delim('['):
		for decoder.More() {
			if err := discardJSON(decoder); err != nil {
				return err
			}
		}
		return delimiter(decoder, ']')
	}
	return nil
}
func jsonEnd(decoder *json.Decoder) error {
	if _, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("unexpected trailing JSON value")
	}
	return nil
}
