package app

import (
	"fmt"
	"net"
	"net/http"
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func (app *Application) configureGRPC(server sink.SinkServer, observed *sinkmetrics.Metrics) error {
	listener, err := net.Listen("tcp", app.config.GRPC.Address)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	serverOptions := make([]grpc.ServerOption, 0, 4)
	serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(app.config.GRPC.MaxReceiveMessageBytes))
	serverOptions = append(serverOptions, grpc.MaxSendMsgSize(app.config.GRPC.MaxSendMessageBytes))
	vtCodec := protocol.NewVTProtoCodec()
	serverOptions = append(serverOptions, grpc.ForceServerCodecV2(vtCodec))
	if observed != nil {
		interceptor := observed.UnaryServerInterceptor()
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(interceptor))
		serverOptions = append(serverOptions, grpc.StreamInterceptor(observed.StreamServerInterceptor()))
	}
	grpcServer := grpc.NewServer(serverOptions...)
	sink.RegisterSinkServer(grpcServer, server)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	for _, configured := range app.healthChecks {
		healthServer.SetServingStatus(configured.service, healthpb.HealthCheckResponse_NOT_SERVING)
	}
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	app.listener = listener
	app.grpcServer = grpcServer
	app.health = healthServer
	return nil
}

func (app *Application) configurePrometheus(handler http.Handler) error {
	listener, err := net.Listen("tcp", app.config.Prometheus.Address)
	if err != nil {
		return fmt.Errorf("listen for Prometheus metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", app.serveReadiness)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	app.metricsListener = listener
	app.metricsServer = server
	return nil
}
