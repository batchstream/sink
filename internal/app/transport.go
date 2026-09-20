package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/liran/sink/internal/config"
	"github.com/liran/sink/internal/forwarding"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	overhead := 0
	if app.config.Mode == config.ModeEngine {
		overhead = forwarding.EnvelopeBytes
	}
	serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(app.config.GRPC.MaxReceiveMessageBytes+overhead))
	serverOptions = append(serverOptions, grpc.MaxSendMsgSize(app.config.GRPC.MaxSendMessageBytes+overhead))
	vtCodec := protocol.NewVTProtoCodec()
	serverOptions = append(serverOptions, grpc.ForceServerCodecV2(vtCodec))
	interceptors := make([]grpc.UnaryServerInterceptor, 0, 2)
	interceptors = append(interceptors, logUnary)
	streamInterceptors := []grpc.StreamServerInterceptor{logStream}
	if app.gateway != nil {
		interceptors = append(interceptors, app.gateway.UnaryInterceptor())
		streamInterceptors = append(streamInterceptors, app.gateway.StreamInterceptor())
	}
	if app.config.Mode == config.ModeEngine {
		store := app.config.Storage.Name
		interceptors = append(interceptors, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if info.FullMethod != "/sink.forward.v1.Engine/Forward" {
				if sized, ok := req.(interface{ SizeVT() int }); ok && sized.SizeVT() > app.config.GRPC.MaxReceiveMessageBytes {
					return nil, status.Error(codes.ResourceExhausted, "request exceeds message limit")
				}
			}
			if err := protocol.CheckStore(req, store); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		})
	}
	if observed != nil {
		interceptor := observed.UnaryServerInterceptor()
		interceptors = append(interceptors, interceptor)
		streamInterceptors = append(streamInterceptors, observed.StreamServerInterceptor())
	}
	serverOptions = append(serverOptions, grpc.ChainUnaryInterceptor(interceptors...))
	serverOptions = append(serverOptions, grpc.ChainStreamInterceptor(streamInterceptors...))
	grpcServer := grpc.NewServer(serverOptions...)
	if app.config.Mode == config.ModeGateway {
		sink.RegisterSinkServer(grpcServer, server)
	}
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

func (app *Application) configureHealth() error {
	listener, err := net.Listen("tcp", app.config.Health.Address)
	if err != nil {
		return fmt.Errorf("listen for health endpoints: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", app.serveReadiness)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	app.healthListener = listener
	app.healthServer = server
	return nil
}

func (app *Application) configurePrometheus(handler http.Handler) error {
	listener, err := net.Listen("tcp", app.config.Prometheus.Address)
	if err != nil {
		return fmt.Errorf("listen for Prometheus metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	app.metricsListener = listener
	app.metricsServer = server
	return nil
}
