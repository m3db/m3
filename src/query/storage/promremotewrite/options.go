package promremotewrite

import (
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

type Options struct {
	endpoints       []EndpointOptions
	requestTimeout  time.Duration
	connectTimeout  time.Duration
	keepAlive       time.Duration
	idleConnTimeout time.Duration
	maxIdleConns    int
}

type EndpointOptions struct {
	address    string
	retention  time.Duration
	resolution time.Duration
}

// TODO add some validation
func NewFromConfiguration(cfg config.PrometheusRemoteWriteBackendConfiguration) Options {
	endpoints := make([]EndpointOptions, len(cfg.Endpoints))

	for i, endpoint := range cfg.Endpoints {
		endpoints[i] = EndpointOptions{
			address:    endpoint.Address,
			resolution: endpoint.Resolution,
			retention:  endpoint.Retention,
		}
	}
	return Options{
		endpoints:       endpoints,
		requestTimeout:  cfg.RequestTimeout,
		connectTimeout:  cfg.ConnectTimeout,
		keepAlive:       cfg.KeepAlive,
		idleConnTimeout: cfg.IdleConnTimeout,
		maxIdleConns:    cfg.MaxIdleConns,
	}
}

func (o Options) HTTPClientOptions() xhttp.HTTPClientOptions {
	clientOpts := xhttp.DefaultHTTPClientOptions()
	if o.requestTimeout != 0 {
		clientOpts.RequestTimeout = o.requestTimeout
	}
	if o.connectTimeout != 0 {
		clientOpts.ConnectTimeout = o.connectTimeout
	}
	if o.keepAlive != 0 {
		clientOpts.KeepAlive = o.keepAlive
	}
	if o.idleConnTimeout != 0 {
		clientOpts.IdleConnTimeout = o.idleConnTimeout
	}
	if o.maxIdleConns != 0 {
		clientOpts.MaxIdleConns = o.maxIdleConns
	}

	clientOpts.DisableCompression = true // Already snappy compressed.
	return clientOpts
}
