package config

import (
	"errors"
	"time"
)

func resolveGateway(file gatewayFile, v *validator) Gateway {
	var loaded Gateway
	loaded.Routes = file.Routes
	if len(loaded.Routes) == 0 || len(loaded.Routes) > 10000 {
		v.reject(errors.New("gateway.routes must contain between 1 and 10000 entries"))
	}
	loaded.DNSRefreshInterval = v.duration("gateway.dns_refresh_interval", file.DNSRefreshInterval, 30*time.Second)
	loaded.IdleTimeout = v.duration("gateway.idle_timeout", file.IdleTimeout, 5*time.Minute)
	loaded.MaxConnections = v.bounded("gateway.max_connections", file.MaxConnections, 256, 10000)
	loaded.MaxRequests = v.bounded("gateway.max_requests", file.MaxRequests, 128, 10000)
	loaded.MaxRequestsPerStore = v.bounded("gateway.max_requests_per_store", file.MaxRequestsPerStore, min(32, loaded.MaxRequests), loaded.MaxRequests)
	loaded.MaxBytes = v.bytes("gateway.max_bytes", file.MaxBytes, 256<<20, 1<<40)
	loaded.MaxFanout = v.bounded("gateway.max_fanout", file.MaxFanout, 8, 128)
	return loaded
}
