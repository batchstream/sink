package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/liran/sink/internal/config"
	storagecontract "github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	searchstorage "github.com/liran/sink/internal/storage/search"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type openedStorage struct {
	value        storagecontract.Storage
	mongoClient  *mongo.Client
	healthChecks []*configuredHealthCheck
}

func openConfiguredStorage(ctx context.Context, loaded config.Config) (openedStorage, error) {
	var opened openedStorage
	configured := loaded.Storage
	backend, err := openStorageBackend(ctx, configured, loaded.ShutdownTimeout)
	if err != nil {
		return opened, fmt.Errorf("open storage %q: %w", configured.Name, err)
	}
	opened.value = backend.value
	opened.mongoClient = backend.mongoClient
	healthCheck := &configuredHealthCheck{service: storageHealthService(configured.Name), pinger: backend.value}
	opened.healthChecks = []*configuredHealthCheck{healthCheck}
	return opened, nil
}

type openedBackend struct {
	value       storagecontract.Storage
	mongoClient *mongo.Client
}

func openStorageBackend(ctx context.Context, configured config.Storage, shutdownTimeout time.Duration) (openedBackend, error) {
	switch configured.Driver {
	case config.DriverMongoDB:
		return openMongoStorage(ctx, configured, shutdownTimeout)
	case config.DriverElasticsearch, config.DriverOpenSearch:
		return openSearchStorage(ctx, configured)
	default:
		var empty openedBackend
		return empty, fmt.Errorf("unsupported storage driver %q", configured.Driver)
	}
}

func openMongoStorage(ctx context.Context, configured config.Storage, shutdownTimeout time.Duration) (openedBackend, error) {
	var opened openedBackend
	journal := true
	concern := &writeconcern.WriteConcern{W: "majority", Journal: &journal}
	clientOptions := options.Client().ApplyURI(configured.MongoDB.URI).SetWriteConcern(concern).SetServerSelectionTimeout(5 * time.Second)
	mongoClient, err := mongo.Connect(clientOptions)
	if err != nil {
		return opened, fmt.Errorf("connect to MongoDB: %w", err)
	}

	storageOptions := mongodb.Options{
		Store:               configured.Name,
		MetadataField:       configured.MongoDB.MetadataField,
		MaxConcurrentWrites: configured.MongoDB.MaxConcurrentWrites,
		MaxConcurrentGroups: configured.MongoDB.MaxConcurrentGroups,
	}
	store, err := mongodb.New(mongoClient, storageOptions)
	if err != nil {
		disconnectContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = mongoClient.Disconnect(disconnectContext)
		return opened, err
	}
	opened.value = store
	opened.mongoClient = mongoClient
	return opened, nil
}

func openSearchStorage(ctx context.Context, configured config.Storage) (openedBackend, error) {
	var opened openedBackend
	searchOptions := searchstorage.Options{
		Driver:    searchstorage.Driver(configured.Driver),
		Endpoints: configured.Search.Endpoints,
		Store:     configured.Name,
		Username:  configured.Search.Username,
		Password:  configured.Search.Password,
		APIKey:    configured.Search.APIKey,
	}
	store, err := searchstorage.New(searchOptions)
	if err != nil {
		return opened, err
	}
	opened.value = store
	return opened, nil
}

func disconnectMongoClient(client *mongo.Client, timeout time.Duration) {
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := client.Disconnect(ctx); err != nil {
		slog.Error("disconnect MongoDB", "error", err)
	}
}
