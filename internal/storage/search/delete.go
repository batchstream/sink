package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/batchstream/sink/internal/storage"
)

type deleteWork struct {
	resultIndex int
	document    resolvedDocument
}

func (s *Store) Delete(ctx context.Context, req storage.DeleteRequest) (storage.DeleteResponse, error) {
	response := storage.DeleteResponse{Results: make([]storage.DeleteResult, len(req.Operations))}
	works := make([]deleteWork, 0, len(req.Operations))
	for index, operation := range req.Operations {
		document, err := s.resolve(operation.Address)
		if err != nil {
			setDeleteError(&response.Results[index], err)
			continue
		}
		work := deleteWork{resultIndex: index, document: document}
		works = append(works, work)
	}
	if len(works) == 0 {
		return response, nil
	}
	request, err := buildDeleteBulk(works)
	if err != nil {
		for _, work := range works {
			setDeleteError(&response.Results[work.resultIndex], err)
		}
		return response, nil
	}
	request.waitUntilVisible = req.WaitUntilVisible
	items, err := s.performBulk(ctx, request)
	if err != nil {
		for _, work := range works {
			setDeleteError(&response.Results[work.resultIndex], err)
		}
		return response, nil
	}
	for index, item := range items {
		result := &response.Results[works[index].resultIndex]
		if item.Error == nil && ((item.Status >= 200 && item.Status < 300) || item.Status == 404) ||
			item.Status == 404 && isIndexNotFound(item.Error) {
			result.Status = storage.DeleteStatusApplied
			continue
		}
		if item.Error != nil {
			setDeleteError(result, classifySearchStatus(item.Status, item.Error))
			continue
		}
		err := fmt.Errorf("search bulk delete returned HTTP %d", item.Status)
		setDeleteError(result, classifySearchStatus(item.Status, err))
	}
	return response, nil
}

func buildDeleteBulk(works []deleteWork) (bulkRequest, error) {
	request := bulkRequest{actions: make([]bulkAction, 0, len(works))}
	var payload bytes.Buffer
	for _, work := range works {
		metadata := bulkActionMetadata{Index: work.document.index, ID: work.document.id}
		action := make(map[string]bulkActionMetadata, 1)
		action["delete"] = metadata
		encodedAction, err := json.Marshal(action)
		if err != nil {
			return request, fmt.Errorf("encode search bulk delete action: %w", err)
		}
		payload.Write(encodedAction)
		payload.WriteByte('\n')
		expected := bulkAction{name: "delete", id: work.document.id}
		request.actions = append(request.actions, expected)
	}
	request.payload = payload.Bytes()
	return request, nil
}

func setDeleteError(result *storage.DeleteResult, err error) {
	result.Status = storage.DeleteStatusFailed
	result.Err = storage.BackendError(err)
}
