package app

import (
	"net/http"

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
	var metricsHandler http.Handler
	if loaded.Prometheus.Enabled {
		metricsHandler = server.MetricsHandler()
	}
	if err := app.configureHTTP(metricsHandler); err != nil {
		return nil, err
	}
	if err := app.configureGRPC(server, nil); err != nil {
		return nil, err
	}
	ready = true
	return app, nil
}
