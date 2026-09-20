package app

import (
	"github.com/batchstream/sink/internal/gateway"
)

func newGateway(opts Options) (*Application, error) {
	loaded := opts.Config
	memory, err := newMemory(loaded)
	if err != nil {
		return nil, err
	}
	gatewayOpts := gateway.Options{Memory: memory, Gateway: loaded.Gateway, Request: loaded.Service.Request, MaxResponseBytes: loaded.GRPC.MaxSendMessageBytes, MaxMessageBytes: max(loaded.GRPC.MaxSendMessageBytes, loaded.GRPC.MaxReceiveMessageBytes)}
	server, err := gateway.New(gatewayOpts)
	if err != nil {
		return nil, err
	}
	app := &Application{config: loaded, gateway: server, memory: memory}
	ready := false
	defer func() {
		if !ready {
			app.Close()
		}
	}()
	if err := app.configureHealth(); err != nil {
		return nil, err
	}
	if loaded.Prometheus.Enabled {
		if err := app.configurePrometheus(server.MetricsHandler()); err != nil {
			return nil, err
		}
	}
	if err := app.configureGRPC(server, nil); err != nil {
		return nil, err
	}
	ready = true
	return app, nil
}
