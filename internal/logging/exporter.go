package logging

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/liran/sink/internal/config"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func newExporter(ctx context.Context, cfg config.LogOTLP) (sdklog.Exporter, error) {
	security := &tls.Config{MinVersion: tls.VersionTLS12}
	headers := make(map[string]string)
	if cfg.Protocol == "http/protobuf" {
		scheme := "https://"
		if !cfg.TLS {
			scheme = "http://"
		}
		retry := otlploghttp.RetryConfig{Enabled: true, InitialInterval: 200 * time.Millisecond, MaxInterval: time.Second, MaxElapsedTime: cfg.ExportTimeout}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = security
		client := &http.Client{Transport: transport, Timeout: cfg.ExportTimeout}
		exporter, err := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(scheme+cfg.Endpoint+"/v1/logs"),
			otlploghttp.WithHeaders(headers), otlploghttp.WithCompression(otlploghttp.NoCompression),
			otlploghttp.WithHTTPClient(client), otlploghttp.WithTimeout(cfg.ExportTimeout), otlploghttp.WithRetry(retry))
		if err != nil {
			transport.CloseIdleConnections()
			return nil, err
		}
		owned := &httpExporter{Exporter: exporter, transport: transport}
		return owned, nil
	}
	transport := credentials.NewTLS(security)
	if !cfg.TLS {
		transport = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(cfg.Endpoint, grpc.WithTransportCredentials(transport))
	if err != nil {
		return nil, err
	}
	retry := otlploggrpc.RetryConfig{Enabled: true, InitialInterval: 200 * time.Millisecond, MaxInterval: time.Second, MaxElapsedTime: cfg.ExportTimeout}
	exporter, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(conn), otlploggrpc.WithHeaders(headers),
		otlploggrpc.WithCompressor(""), otlploggrpc.WithTimeout(cfg.ExportTimeout), otlploggrpc.WithRetry(retry))
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	owned := &grpcExporter{Exporter: exporter, connection: conn}
	return owned, nil
}

type httpExporter struct {
	sdklog.Exporter
	transport *http.Transport
}

func (e *httpExporter) Shutdown(ctx context.Context) error {
	err := e.Exporter.Shutdown(ctx)
	e.transport.CloseIdleConnections()
	return err
}

type grpcExporter struct {
	sdklog.Exporter
	connection *grpc.ClientConn
}

func (e *grpcExporter) Shutdown(ctx context.Context) error {
	err := e.Exporter.Shutdown(ctx)
	_ = e.connection.Close()
	return err
}
