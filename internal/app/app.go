// Package app assembles Sink dependencies and owns their process lifecycle.
package app

import (
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/liran/sink/internal/config"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
	queuekafka "github.com/liran/sink/internal/queue/kafka"
	"github.com/liran/sink/internal/service"
	storagecontract "github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
)

type Application struct {
	topics          map[string]*queuekafka.TopicManager
	background      sync.WaitGroup
	config          config.Config
	mongoClients    map[string]*mongo.Client
	storage         storagecontract.Storage
	publisher       queue.Publisher
	kafkaPublishers []*queuekafka.Publisher
	healthChecks    []*configuredHealthCheck
	workers         []configuredWorker
	batchingServer  *service.BatchingServer
	grpcServer      *grpc.Server
	health          *health.Server
	listener        net.Listener
	metricsServer   *http.Server
	metricsListener net.Listener
}

type Options struct {
	Config  config.Config
	Version string
}

// New assembles a configuration returned by config.Load or config.Decode.
// Dependencies recover independently after Run starts. On an assembly error,
// every resource opened by this call is closed before returning.
func New(ctx context.Context, opts Options) (*Application, error) {
	loaded := opts.Config
	opened, err := openConfiguredStorage(ctx, loaded)
	if err != nil {
		return nil, err
	}
	app := &Application{
		config:       loaded,
		topics:       make(map[string]*queuekafka.TopicManager),
		mongoClients: opened.mongoClients,
		storage:      opened.value,
		healthChecks: opened.healthChecks,
	}
	ready := false
	defer func() {
		if !ready {
			app.Close()
		}
	}()
	var observed *sinkmetrics.Metrics
	if loaded.Prometheus.Address != "" {
		names := make([]string, len(loaded.Storages))
		for index, storage := range loaded.Storages {
			names[index] = storage.Name
		}
		observed, err = sinkmetrics.New(opts.Version, names...)
		if err != nil {
			return nil, err
		}
		if err := app.configurePrometheus(observed.Handler()); err != nil {
			return nil, err
		}
	}
	if err := app.configureKafka(observed); err != nil {
		return nil, err
	}
	server, err := app.newService(observed)
	if err != nil {
		return nil, err
	}
	if loaded.Mode == config.ModeServer || loaded.Mode == config.ModeAll {
		if err := app.configureServer(server, observed); err != nil {
			return nil, err
		}
	}
	if loaded.Mode == config.ModeWorker || loaded.Mode == config.ModeAll {
		if err := app.configureWorkers(server, observed); err != nil {
			return nil, err
		}
	}
	ready = true
	return app, nil
}
