package app

import (
	"github.com/liran/sink/internal/merge"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/service"
)

func (app *Application) newService(observed *sinkmetrics.Metrics) (*service.Server, error) {
	loaded := app.config
	storeExecutionBytes := make(map[string]int)
	storeNames := make([]string, len(loaded.Storages))
	for index, configured := range loaded.Storages {
		storeNames[index] = configured.Name
		if configured.Limits.MaxExecutionBytes > 0 {
			storeExecutionBytes[configured.Name] = configured.Limits.MaxExecutionBytes
		}
	}
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
		StoreNames:                storeNames,
		RequestTimeout:            loaded.Service.Request.Timeout,
		MaxInFlightRequests:       loaded.Service.Execution.MaxRequests,
		MaxInFlightBytes:          loaded.Service.Execution.MaxBytes,
		MaxAdmissionRequests:      loaded.Service.Execution.Queue.MaxRequests,
		MaxAdmissionBytes:         loaded.Service.Execution.Queue.MaxBytes,
		MaxStoreAdmissionRequests: loaded.Service.Execution.Queue.MaxRequestsPerStore,
		AdmissionWait:             loaded.Service.Execution.Queue.MaxWait,
		MaxPublishRequests:        loaded.Service.Publish.MaxRequests,
		MaxPublishStoreRequests:   loaded.Service.Publish.MaxRequestsPerStore,
		MaxPublishBytes:           loaded.Service.Publish.MaxBytes,
		MaxStoreRequests:          loaded.Service.Execution.MaxRequestsPerStore,
		MaxScanRequests:           loaded.Service.Execution.Scan.MaxRequests,
		MaxScanBytes:              loaded.Service.Execution.Scan.MaxBytes,
		MaxStoreScanRequests:      loaded.Service.Execution.Scan.MaxRequestsPerStore,
		ScanAdmissionWait:         loaded.Service.Execution.Scan.AdmissionWait,
		StoreExecutionBytes:       storeExecutionBytes,
		MaxReadBytes:              loaded.Service.Request.MaxReadBytes,
		Storage:                   app.storage,
		Lua:                       luaEngine,
		Publisher:                 app.publisher,
		MaxOperations:             loaded.Service.Request.MaxOperations,
		MaxMergeAttempts:          loaded.Service.Merge.MaxAttempts,
		Metrics:                   observed,
	}
	return service.New(serverOptions)
}

func (app *Application) configureServer(sinkServer *service.Server, observed *sinkmetrics.Metrics) error {
	loaded := app.config
	storeNames := make([]string, len(loaded.Storages))
	for index, storage := range loaded.Storages {
		storeNames[index] = storage.Name
	}
	var err error

	batchingOptions := service.BatchingOptions{
		StoreNames:          storeNames,
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
	return app.configureGRPC(app.batchingServer, observed)
}
