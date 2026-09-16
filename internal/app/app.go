// Package app assembles Sink dependencies and owns their process lifecycle.
package app

import (
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/gateway"
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
	gateway         *gateway.Server
	topics          *queuekafka.TopicManager
	background      sync.WaitGroup
	config          config.Config
	mongoClient     *mongo.Client
	storage         storagecontract.Storage
	publisher       queue.Publisher
	kafkaPublisher  *queuekafka.Publisher
	healthChecks    []*configuredHealthCheck
	worker          *queuekafka.Worker
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
	if loaded.Mode == config.ModeGateway {
		return newGateway(opts)
	}
	opened, err := openConfiguredStorage(ctx, loaded)
	if err != nil {
		return nil, err
	}
	app := &Application{
		config:       loaded,
		mongoClient:  opened.mongoClient,
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
	if loaded.Prometheus.Enabled {
		observed, err = sinkmetrics.New(opts.Version, loaded.Storage.Name)
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
	if loaded.Mode == config.ModeEngine {
		if err := app.configureServer(server, observed); err != nil {
			return nil, err
		}
	}
	if loaded.Mode == config.ModeWorker {
		if err := app.configureWorker(server, observed); err != nil {
			return nil, err
		}
	}
	ready = true
	return app, nil
}
