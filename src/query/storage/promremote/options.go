package promremote

import (
	"errors"
	"fmt"
	"strings"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

// NewOptions constructs options given config.
func NewOptions(cfg config.PrometheusRemoteBackendConfiguration) (Options, error) {
	err := validateBackendConfiguration(cfg)
	if err != nil {
		return Options{}, err
	}
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
	}, nil
}

// HTTPClientOptions maps options to http client options
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

func validateBackendConfiguration(cfg config.PrometheusRemoteBackendConfiguration) error {
	if len(cfg.Endpoints) == 0 {
		return fmt.Errorf(
			"at least one endpoint must be configured when using %s backend type",
			config.PromRemoteStorageType,
		)
	}
	if cfg.MaxIdleConns < 0 {
		return errors.New("maxIdleConns can't be negative")
	}
	if cfg.KeepAlive < 0 {
		return errors.New("keepAlive can't be negative")
	}
	if cfg.IdleConnTimeout < 0 {
		return errors.New("idleConnTimeout can't be negative")
	}
	if cfg.RequestTimeout < 0 {
		return errors.New("requestTimeout can't be negative")
	}
	if cfg.ConnectTimeout < 0 {
		return errors.New("connectTimeout can't be negative")
	}

	for _, endpoint := range cfg.Endpoints {
		if err := validateEndpointConfiguration(endpoint); err != nil {
			return err
		}
	}
	return nil
}

func validateEndpointConfiguration(endpoint config.PrometheusRemoteBackendEndpointConfiguration) error {
	if endpoint.Resolution < 0 {
		return errors.New("endpoint resolution can't be negative")
	}
	if endpoint.Retention < 0 {
		return errors.New("endpoint retention can't be negative")
	}
	if strings.TrimSpace(endpoint.Address) == "" {
		return errors.New("endpoint address must be set")
	}
	return nil
}
