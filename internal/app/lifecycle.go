package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

func (app *Application) Run(ctx context.Context) error {
	runContext, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
		app.background.Wait()
	}()
	if app.gateway != nil {
		app.background.Go(func() { app.gateway.Run(runContext) })
	}
	if app.topics != nil {
		app.background.Go(func() { app.topics.Run(runContext) })
	}
	runErrors := make(chan error, 4)
	if app.grpcServer != nil {
		go func() {
			err := app.grpcServer.Serve(app.listener)
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				runErrors <- fmt.Errorf("serve gRPC: %w", err)
			}
		}()
	}
	if app.healthServer != nil {
		go func() {
			err := app.healthServer.Serve(app.healthListener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				runErrors <- fmt.Errorf("serve health endpoints: %w", err)
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
	if app.worker != nil {
		app.background.Go(func() {
			if err := app.worker.Run(runContext); err != nil {
				runErrors <- fmt.Errorf("run Kafka worker for store %q: %w", app.config.Storage.Name, err)
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
	if app.gateway != nil {
		app.gateway.Close()
	}
	if app.batchingServer != nil {
		app.batchingServer.Close()
	}
	if app.healthServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), app.config.ShutdownTimeout)
		defer cancel()
		if err := app.healthServer.Shutdown(ctx); err != nil {
			slog.Error("shut down health endpoints", "error", err)
			_ = app.healthServer.Close()
		}
	}
	if app.healthListener != nil {
		_ = app.healthListener.Close()
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
	if app.worker != nil {
		app.worker.Close()
	}
	if app.kafkaPublisher != nil {
		app.kafkaPublisher.Close()
	}
	disconnectMongoClient(app.mongoClient, app.config.ShutdownTimeout)
}
