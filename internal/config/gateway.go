package config

import (
	"errors"
	"time"
)

func resolveGateway(file gatewayFile, v *validator) Gateway {
	var loaded Gateway
	loaded.Routes = file.Routes
	if len(loaded.Routes) == 0 || len(loaded.Routes) > 10000 {
		v.reject(errors.New("forwarding.routes must contain between 1 and 10000 entries"))
	}
	loaded.DNSRefreshInterval = v.duration("forwarding.dns_refresh_interval", file.DNSRefreshInterval, 30*time.Second)
	loaded.IdleTimeout = v.duration("forwarding.idle_timeout", file.IdleTimeout, 5*time.Minute)
	loaded.MaxConnections = v.bounded("forwarding.max_connections", file.MaxConnections, 256, 10000)
	loaded.MaxFanout = v.bounded("forwarding.max_fanout", file.MaxFanout, 8, 128)
	return loaded
}
