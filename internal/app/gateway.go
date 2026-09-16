package app

import (
	"github.com/liran/sink/internal/gateway"
)

func newGateway(opts Options) (*Application, error) {
	loaded := opts.Config
	gatewayOpts := gateway.Options{Gateway: loaded.Gateway, Request: loaded.Service.Request, MaxMessageBytes: max(loaded.GRPC.MaxSendMessageBytes, loaded.GRPC.MaxReceiveMessageBytes)}
	server, err := gateway.New(gatewayOpts)
	if err != nil {
		return nil, err
	}
	app := &Application{config: loaded, gateway: server}
	ready := false
	defer func() {
		if !ready {
			app.Close()
		}
	}()
	if loaded.Prometheus.Address != "" {
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
