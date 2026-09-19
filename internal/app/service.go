package app

import (
	forward "github.com/liran/sink/gen/forward"
	"github.com/liran/sink/internal/engine"
	"github.com/liran/sink/internal/merge"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/service"
)

func (app *Application) newService(observed *sinkmetrics.Metrics) (*service.Server, error) {
	loaded := app.config
	lua := loaded.Service.Merge.Lua
	luaOptions := merge.LuaOptions{
		Timeout:           lua.Timeout,
		MaxSourceBytes:    lua.MaxSourceBytes,
		MaxResultBytes:    lua.MaxResultBytes,
		MaxCachedPrograms: lua.MaxCachedPrograms,
		MaxInstructions:   int64(lua.MaxInstructions),
	}
	luaEngine, err := merge.NewLuaEngine(luaOptions)
	if err != nil {
		return nil, err
	}
	serverOptions := service.Options{
		MaxReadBytes:     loaded.GRPC.MaxSendMessageBytes,
		Storage:          app.storage,
		Lua:              luaEngine,
		Publisher:        app.publisher,
		MaxOperations:    0,
		MaxMergeAttempts: loaded.Service.Merge.MaxAttempts,
		Metrics:          observed,
	}
	serverOptions.BoundStore = loaded.Storage.Name
	return service.New(serverOptions)
}

func (app *Application) configureServer(sinkServer *service.Server, observed *sinkmetrics.Metrics) error {
	loaded := app.config
	var err error

	batchingOptions := service.BatchingOptions{
		MaxWait:             loaded.Service.Batching.MaxWait,
		MaxOperations:       loaded.Service.Batching.MaxOperations,
		MaxBytes:            loaded.Service.Batching.MaxBytes,
		MaxQueuedOperations: loaded.Service.Batching.Queue.MaxOperations,
		MaxQueuedBytes:      loaded.Service.Batching.Queue.MaxBytes,
		Metrics:             observed,
	}
	app.batchingServer, err = service.NewBatchingServer(sinkServer, batchingOptions)
	if err != nil {
		return err
	}
	if err := app.configureGRPC(app.batchingServer, observed); err != nil {
		return err
	}
	opts := engine.Options{Memory: app.memory, MaxRequestBytes: loaded.GRPC.MaxReceiveMessageBytes, Metrics: observed, Service: app.batchingServer, Store: loaded.Storage.Name, MaxReadBytes: loaded.GRPC.MaxSendMessageBytes}
	forwardingServer, err := engine.New(opts)
	if err != nil {
		return err
	}
	forward.RegisterEngineServer(app.grpcServer, forwardingServer)
	return nil
}
