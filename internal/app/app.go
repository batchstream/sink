// Package app assembles Sink dependencies and owns their process lifecycle.
package app

import (
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/batchstream/sink/internal/backpressure"
	"github.com/batchstream/sink/internal/capacity"
	"github.com/batchstream/sink/internal/config"
	"github.com/batchstream/sink/internal/gateway"
	sinkmetrics "github.com/batchstream/sink/internal/metrics"
	"github.com/batchstream/sink/internal/queue"
	queuekafka "github.com/batchstream/sink/internal/queue/kafka"
	"github.com/batchstream/sink/internal/service"
	storagecontract "github.com/batchstream/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
)

type Application struct {
	admission       *backpressure.Controller
	memory          *capacity.Guard
	draining        atomic.Bool
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
	healthServer    *http.Server
	healthListener  net.Listener
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
	memory, err := newMemory(loaded)
	if err != nil {
		return nil, err
	}
	opened, err := openConfiguredStorage(ctx, loaded)
	if err != nil {
		return nil, err
	}
	app := &Application{
		config:       loaded,
		memory:       memory,
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
	admissionOptions := backpressure.Options{
		Store: loaded.Storage.Name, Role: string(loaded.Mode), MaxConcurrent: loaded.Service.StoreMaxConcurrent,
		MaxQueuedRequests: loaded.Service.Batching.Queue.MaxOperations,
		MaxQueuedBytes:    loaded.Service.Batching.Queue.MaxBytes,
	}
	app.admission, err = backpressure.New(admissionOptions)
	if err != nil {
		return nil, err
	}
	var observed *sinkmetrics.Metrics
	if err := app.configureHealth(); err != nil {
		return nil, err
	}
	if loaded.Prometheus.Enabled {
		observed, err = sinkmetrics.New(opts.Version, loaded.Storage.Name)
		if err != nil {
			return nil, err
		}
		if err := observed.Register(memory); err != nil {
			return nil, err
		}
		if err := observed.Register(app.admission); err != nil {
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
