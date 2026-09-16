// Package mongodb implements document storage backed by MongoDB.
package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/liran/sink-go/uri"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	defaultMetadataField    = "__sink"
	defaultConcurrentWrites = 64
	defaultConcurrentGroups = 16
)

type Options struct {
	Store               string
	MetadataField       string
	MaxConcurrentWrites int
	MaxConcurrentGroups int
}

type Store struct {
	client               *mongo.Client
	store                string
	metadataField        string
	maxConcurrentWrites  int
	maxConcurrentGroups  int
	groups               chan struct{}
	writes               chan struct{}
	clientBulkCapability atomic.Uint32
}

func New(client *mongo.Client, opts Options) (*Store, error) {
	if client == nil {
		return nil, errors.New("create MongoDB storage: client is required")
	}
	if opts.Store == "" {
		return nil, errors.New("create MongoDB storage: logical store is required")
	}
	if opts.MaxConcurrentWrites < 0 || opts.MaxConcurrentGroups < 0 {
		return nil, errors.New("create MongoDB storage: concurrency limits cannot be negative")
	}

	metadataField := opts.MetadataField
	if metadataField == "" {
		metadataField = defaultMetadataField
	}
	if metadataField == "_id" || strings.ContainsAny(metadataField, ".$\x00") {
		return nil, errors.New("create MongoDB storage: metadata field is invalid")
	}

	maxConcurrentWrites := opts.MaxConcurrentWrites
	if maxConcurrentWrites == 0 {
		maxConcurrentWrites = defaultConcurrentWrites
	}
	maxConcurrentGroups := opts.MaxConcurrentGroups
	if maxConcurrentGroups == 0 {
		maxConcurrentGroups = defaultConcurrentGroups
	}
	store := &Store{
		client:              client,
		store:               opts.Store,
		metadataField:       metadataField,
		maxConcurrentWrites: maxConcurrentWrites,
		maxConcurrentGroups: maxConcurrentGroups,
		groups:              make(chan struct{}, maxConcurrentGroups),
		writes:              make(chan struct{}, maxConcurrentWrites),
	}
	return store, nil
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx, nil); err != nil {
		return storage.BackendError(err)
	}
	return nil
}

type resolvedCollection struct {
	database   string
	collection string
	value      *mongo.Collection
	recordKey  storage.Key
}

func (s *Store) resolve(address storage.Address) (resolvedCollection, error) {
	var resolved resolvedCollection
	if address.Store() != s.store {
		err := fmt.Errorf("logical store %q is not configured", address.Store())
		return resolved, storage.InvalidArgumentError(err)
	}
	segments := address.Segments()
	if len(segments) != 3 {
		return resolved, storage.InvalidArgumentError(errors.New("MongoDB record URI requires database/collection/typed-key"))
	}
	if strings.ContainsAny(segments[0], "/\\. \"$\x00") || strings.ContainsRune(segments[1], '\x00') {
		return resolved, storage.InvalidArgumentError(errors.New("invalid MongoDB database or collection name"))
	}
	key, err := uri.ParseKey(segments[2])
	if err != nil {
		return resolved, storage.InvalidArgumentError(err)
	}
	if _, err := mongoID(key); err != nil {
		return resolved, storage.InvalidArgumentError(err)
	}
	resolved.database = segments[0]
	resolved.collection = segments[1]
	resolved.recordKey = key
	resolved.value = s.client.Database(segments[0]).Collection(segments[1])

	return resolved, nil
}

func (r resolvedCollection) key() string {
	return r.database + "\x00" + r.collection
}

func (s *Store) BatchKey(address storage.Address) (string, error) {
	resolved, err := s.resolve(address)
	if err != nil {
		return "", err
	}
	return resolved.key(), nil
}
