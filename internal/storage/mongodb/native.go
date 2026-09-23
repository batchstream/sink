package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"strings"
	"time"

	"github.com/batchstream/sink-protocol/uri"
	"github.com/batchstream/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func (s *Store) validateNativeCommand(req storage.NativeRequest, scan bool) (string, bson.D, error) {
	var command bson.D
	address, err := uri.Parse(req.URI)
	if err != nil {
		return "", command, err
	}
	parts := address.Segments()
	if address.Store() != s.store || len(parts) < 1 || len(parts) > 2 || strings.TrimSpace(parts[0]) == "" || strings.ContainsAny(parts[0], "/\\. \"$\x00") {
		return "", command, errors.New("MongoDB native URI requires the configured Store and database[/collection]")
	}
	database := parts[0]
	collection := ""
	if len(parts) == 2 {
		collection = parts[1]
		if strings.ContainsRune(collection, '\x00') {
			return database, command, errors.New("invalid MongoDB collection name")
		}
	}
	if req.Method != "" || req.Path != "" || req.Query != "" || len(req.Headers) != 0 {
		return database, command, errors.New("MongoDB native requests do not use method, path, query or headers")
	}
	if req.ContentType != "" || len(req.Payload) > 0 {
		mediaType, _, err := mime.ParseMediaType(req.ContentType)
		if err != nil || mediaType != "application/bson" {
			return database, command, errors.New("MongoDB native requests require application/bson content_type")
		}
	}
	if len(req.Payload) == 0 && scan && collection != "" {
		command = bson.D{{Key: "find", Value: collection}}
	} else {
		if err := storage.ValidateBSONDocument(req.Payload); err != nil {
			return database, command, fmt.Errorf("invalid BSON command: %w", err)
		}
		if err := bson.Unmarshal(req.Payload, &command); err != nil {
			return database, command, fmt.Errorf("decode BSON command: %w", err)
		}
	}
	if len(command) == 0 {
		return database, command, errors.New("BSON command is empty")
	}
	if collection != "" {
		switch command[0].Key {
		case "find", "aggregate", "listIndexes", "insert", "update", "delete", "findAndModify", "findandmodify",
			"count", "distinct", "createIndexes", "dropIndexes", "collStats":
		default:
			return database, command, errors.New("command requires a database URI, not a collection URI")
		}
		target, ok := command[0].Value.(string)
		if !ok || (target != "" && target != collection) {
			return database, command, errors.New("native command must target the URI collection")
		}
		command[0].Value = collection
	}
	seen := make(map[string]bool, len(command))
	for _, field := range command {
		if seen[field.Key] {
			return database, command, fmt.Errorf("duplicate command field %q", field.Key)
		}
		seen[field.Key] = true
		switch field.Key {
		case "$db", "lsid", "txnNumber", "startTransaction", "autocommit", "apiVersion", "apiStrict", "apiDeprecationErrors", "maxTimeMS", "$readPreference":
			return database, command, fmt.Errorf("command field %q is managed by Sink's driver", field.Key)
		}
	}
	name := command[0].Key
	if scan {
		switch name {
		case "find", "aggregate", "listIndexes", "listCollections":
		default:
			return database, command, fmt.Errorf("command %q cannot be scanned", name)
		}
		if forbiddenNativeQuery(command) {
			return database, command, errors.New("scan does not permit data-writing stages or tailable cursors")
		}
		for _, field := range command {
			if field.Key == "allowPartialResults" && field.Value != false {
				return database, command, errors.New("native queries require complete results from every shard")
			}
		}
		return database, command, nil
	}
	switch name {
	case "find", "aggregate", "listIndexes", "listCollections", "getMore", "killCursors", "parallelCollectionScan", "bulkWrite":
		return database, command, fmt.Errorf("command %q uses a cursor; Execute does not manage cursor sessions, use Scan for supported cursor queries", name)
	case "startSession", "refreshSessions", "endSessions", "commitTransaction", "abortTransaction":
		return database, command, fmt.Errorf("command %q requires client-managed sessions, which Execute does not support", name)
	case "insert", "update", "delete", "findAndModify", "findandmodify",
		"count", "distinct", "explain", "createIndexes", "dropIndexes",
		"collStats", "dbStats", "ping", "hello", "isMaster", "ismaster", "buildInfo", "serverStatus":
		return database, command, nil
	default:
		return database, command, fmt.Errorf("command %q is not supported by revision-protected MongoDB Execute", name)
	}
}

func (s *Store) Execute(ctx context.Context, req storage.NativeRequest) (storage.NativeResponse, error) {
	var empty storage.NativeResponse
	database, command, err := s.validateNativeCommand(req, false)
	if err != nil {
		return empty, storage.InvalidArgumentError(err)
	}
	command, err = s.prepareNativeWrite(command)
	if err != nil {
		return empty, err
	}
	raw, err := s.client.Database(database).RunCommand(ctx, command).Raw()
	if len(raw) == 0 {
		var commandError mongo.CommandError
		if errors.As(err, &commandError) {
			raw = commandError.Raw
		}
		var writeError mongo.WriteException
		if errors.As(err, &writeError) {
			raw = writeError.Raw
		}
	}
	if len(raw) == 0 {
		if err == nil {
			err = errors.New("MongoDB command returned an empty response")
		}
		return empty, storage.BackendError(err)
	}
	budget := storage.NewReadBudget(req.MaxBytes)
	if budgetErr := budget.Reserve(len(raw)); budgetErr != nil {
		return empty, budgetErr
	}
	response := storage.NativeResponse{ContentType: "application/bson", Payload: bytes.Clone(raw), Success: err == nil, Failure: nativeResponseFailure(raw, err)}
	return response, nil
}

func nativeResponseFailure(raw bson.Raw, err error) error {
	if err != nil {
		return nativeFailure(err)
	}
	// RunCommand can return ok:1 with per-item errors or an uncertain write
	// concern. Normalize those too without changing the raw reply or Success.
	if concern, ok := raw.Lookup("writeConcernError").DocumentOK(); ok {
		var failure mongo.WriteConcernError
		if bson.Unmarshal(concern, &failure) == nil {
			return storage.BackendError(failure)
		}
	}
	items, ok := raw.Lookup("writeErrors").ArrayOK()
	if !ok {
		return nil
	}
	values, err := items.Values()
	if err != nil {
		return nil
	}
	var first error
	for _, value := range values {
		document, ok := value.DocumentOK()
		if !ok {
			continue
		}
		var item mongo.WriteError
		if bson.Unmarshal(document, &item) != nil {
			continue
		}
		commandError := mongo.CommandError{Code: int32(item.Code), Message: item.Message}
		failure := nativeFailure(commandError)
		if first == nil {
			first = failure
		}
		if _, retryable := storage.ErrorDetails(failure); retryable {
			return failure
		}
	}
	return first
}

// Native errors remain in their original wire response. Supply only confirmed
// dependency/timeout classifications; arbitrary command failures are not evidence
// of overload (and must not become new transport errors or retry instructions).
func nativeFailure(err error) error {
	if err == nil {
		return nil
	}
	if mongo.IsTimeout(err) {
		return storage.NewOperationError(storage.ErrorCodeDeadlineExceeded, true, err)
	}
	if mongo.IsDuplicateKeyError(err) {
		return storage.NewOperationError(storage.ErrorCodePreconditionFailed, false, err)
	}
	var serverError mongo.ServerError
	if errors.As(err, &serverError) {
		for _, code := range []int{6, 7, 89, 91, 189, 10107, 11600, 11602, 13435, 13436} {
			if serverError.HasErrorCode(code) {
				return storage.BackendError(err)
			}
		}
		if serverError.HasErrorCode(16500) {
			return storage.ResourceExhaustedError(err)
		}
	}
	return err
}

func scanCommand(command bson.D, batchSize int) bson.D {
	name := command[0].Key
	filtered := make(bson.D, 0, len(command)+1)
	for _, field := range command {
		if field.Key == "batchSize" || (name != "find" && field.Key == "cursor") {
			continue
		}
		filtered = append(filtered, field)
	}
	batch := bson.E{Key: "batchSize", Value: int32(batchSize)}
	if name == "find" {
		filtered = append(filtered, batch)
	} else {
		cursor := bson.D{batch}
		field := bson.E{Key: "cursor", Value: cursor}
		filtered = append(filtered, field)
	}
	return filtered
}

func (s *Store) scanDocuments(ctx context.Context, req storage.ScanRequest, send func([]storage.Document) error) error {
	if req.BatchSize < 1 || req.BatchSize > 1000 {
		return storage.InvalidArgumentError(errors.New("scan batch size must be between 1 and 1000"))
	}
	database, command, err := s.validateNativeCommand(req.Request, true)
	if err != nil {
		return storage.InvalidArgumentError(err)
	}
	command = scanCommand(command, req.BatchSize)
	cursor, err := s.client.Database(database).RunCommandCursor(ctx, command)
	if err != nil {
		return storage.BackendError(err)
	}
	cursor.SetBatchSize(int32(req.BatchSize))
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = cursor.Close(cleanup)
	}()
	batch := make([]storage.Document, 0, req.BatchSize)
	// Count consumes each page synchronously; these documents never become RPC
	// output. Discard each page before reading the next.
	budget := storage.NewReadBudget(req.Request.MaxBytes)
	for cursor.Next(ctx) {
		if err := budget.Reserve(len(cursor.Current)); err != nil {
			if len(batch) == 0 {
				return err
			}
			if err := send(batch); err != nil {
				return err
			}
			batch = make([]storage.Document, 0, req.BatchSize)
			budget = storage.NewReadBudget(req.Request.MaxBytes)
			if err := budget.Reserve(len(cursor.Current)); err != nil {
				return err
			}
		}
		document := storage.Document{Encoding: storage.DocumentEncodingBSON, Payload: bytes.Clone(cursor.Current)}
		batch = append(batch, document)
		if len(batch) == req.BatchSize {
			if err := send(batch); err != nil {
				return err
			}
			batch = make([]storage.Document, 0, req.BatchSize)
			budget = storage.NewReadBudget(req.Request.MaxBytes)
		}
	}
	if err := cursor.Err(); err != nil {
		return storage.BackendError(err)
	}
	if len(batch) != 0 {
		return send(batch)
	}
	return nil
}
