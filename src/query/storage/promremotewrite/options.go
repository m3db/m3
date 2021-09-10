package promremotewrite

import (
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
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
	storageMetadata storagemetadata.Attributes
}

// TODO add some validation
func NewFromConfiguration(cfg config.PrometheusRemoteWriteBackendConfiguration) Options {
	endpoints := make([]EndpointOptions, len(cfg.Endpoints))

	for i, endpoint := range cfg.Endpoints {
		storageMeta := storagemetadata.Attributes{
			MetricsType: endpoint.Type,
		}
		if endpoint.Type == storagemetadata.AggregatedMetricsType {
			storageMeta.Resolution = endpoint.Resolution
			storageMeta.Retention = endpoint.Retention
		}
		endpoints[i] = EndpointOptions{
			address:         endpoint.Address,
			storageMetadata: storageMeta,
		}
	}
	return Options{
		endpoints: endpoints,
		requestTimeout:  cfg.RequestTimeout,
		connectTimeout:  cfg.ConnectTimeout,
		keepAlive:       cfg.KeepAlive,
		idleConnTimeout: cfg.IdleConnTimeout,
		maxIdleConns:    cfg.MaxIdleConns,
	}
}

func (o *Options) HTTPClientOptions() xhttp.HTTPClientOptions {
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
