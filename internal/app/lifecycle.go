package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	queuekafka "github.com/liran/sink/internal/queue/kafka"
	"google.golang.org/grpc"
)

type configuredWorker struct {
	store  string
	worker *queuekafka.Worker
}

func (app *Application) Run(ctx context.Context) error {
	runContext, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		app.background.Wait()
	}()
	for _, manager := range app.topics {
		app.background.Go(func() { manager.Run(runContext) })
	}
	runErrors := make(chan error, 2+len(app.workers))
	if app.grpcServer != nil {
		go func() {
			err := app.grpcServer.Serve(app.listener)
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				runErrors <- fmt.Errorf("serve gRPC: %w", err)
			}
		}()
	}
	if app.metricsServer != nil {
		go func() {
			err := app.metricsServer.Serve(app.metricsListener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				runErrors <- fmt.Errorf("serve Prometheus metrics: %w", err)
			}
		}()
	}
	for _, configured := range app.workers {
		app.background.Go(func() {
			if err := configured.worker.Run(runContext); err != nil {
				runErrors <- fmt.Errorf("run Kafka worker for store %q: %w", configured.store, err)
			}
		})
	}
	if app.health != nil {
		app.background.Go(func() { app.runHealthChecks(runContext) })
	}
	select {
	case <-runContext.Done():
		return nil
	case err := <-runErrors:
		return err
	}
}

func (app *Application) Close() {
	if app.health != nil {
		app.health.Shutdown()
	}
	if app.grpcServer != nil {
		stopped := make(chan struct{})
		go func() {
			app.grpcServer.GracefulStop()
			close(stopped)
		}()
		timer := time.NewTimer(app.config.ShutdownTimeout)
		select {
		case <-stopped:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			app.grpcServer.Stop()
		}
	}
	if app.listener != nil {
		_ = app.listener.Close()
	}
	if app.batchingServer != nil {
		app.batchingServer.Close()
	}
	if app.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), app.config.ShutdownTimeout)
		defer cancel()
		if err := app.metricsServer.Shutdown(ctx); err != nil {
			slog.Error("shut down Prometheus metrics", "error", err)
			_ = app.metricsServer.Close()
		}
	}
	if app.metricsListener != nil {
		_ = app.metricsListener.Close()
	}
	for _, configured := range app.workers {
		configured.worker.Close()
	}
	for _, publisher := range app.kafkaPublishers {
		publisher.Close()
	}
	disconnectMongoClients(app.mongoClients, app.config.ShutdownTimeout)
}
