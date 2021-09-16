// Copyright (c) 2021  Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package promremote

import (
	"errors"
	"fmt"
	"strings"

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

// NewOptions constructs options given config.
func NewOptions(cfg *config.PrometheusRemoteBackendConfiguration, scope tally.Scope) (Options, error) {
	err := validateBackendConfiguration(cfg)
	if err != nil {
		return Options{}, err
	}
	endpoints := make([]EndpointOptions, len(cfg.Endpoints))

	for i, endpoint := range cfg.Endpoints {
		endpoints[i] = EndpointOptions{
			name:       endpoint.Name,
			address:    endpoint.Address,
			resolution: endpoint.Resolution,
			retention:  endpoint.Retention,
		}
	}
	clientOpts := xhttp.DefaultHTTPClientOptions()
	if cfg.RequestTimeout != nil {
		clientOpts.RequestTimeout = *cfg.RequestTimeout
	}
	if cfg.ConnectTimeout != nil {
		clientOpts.ConnectTimeout = *cfg.ConnectTimeout
	}
	if cfg.KeepAlive != nil {
		clientOpts.KeepAlive = *cfg.KeepAlive
	}
	if cfg.IdleConnTimeout != nil {
		clientOpts.IdleConnTimeout = *cfg.IdleConnTimeout
	}
	if cfg.MaxIdleConns != nil {
		clientOpts.MaxIdleConns = *cfg.MaxIdleConns
	}

	clientOpts.DisableCompression = true // Already snappy compressed.

	return Options{
		endpoints:   endpoints,
		httpOptions: clientOpts,
		scope:       scope,
	}, nil
}

func validateBackendConfiguration(cfg *config.PrometheusRemoteBackendConfiguration) error {
	if cfg == nil {
		return fmt.Errorf("prometheusRemoteBackend configuration is required")
	}
	if len(cfg.Endpoints) == 0 {
		return fmt.Errorf(
			"at least one endpoint must be configured when using %s backend type",
			config.PromRemoteStorageType,
		)
	}
	if cfg.MaxIdleConns != nil && *cfg.MaxIdleConns < 0 {
		return errors.New("maxIdleConns can't be negative")
	}
	if cfg.KeepAlive != nil && *cfg.KeepAlive < 0 {
		return errors.New("keepAlive can't be negative")
	}
	if cfg.IdleConnTimeout != nil && *cfg.IdleConnTimeout < 0 {
		return errors.New("idleConnTimeout can't be negative")
	}
	if cfg.RequestTimeout != nil && *cfg.RequestTimeout < 0 {
		return errors.New("requestTimeout can't be negative")
	}
	if cfg.ConnectTimeout != nil && *cfg.ConnectTimeout < 0 {
		return errors.New("connectTimeout can't be negative")
	}

	seenNames := map[string]struct{}{}
	for _, endpoint := range cfg.Endpoints {
		if err := validateEndpointConfiguration(endpoint); err != nil {
			return err
		}
		if _, ok := seenNames[endpoint.Name]; ok {
			return fmt.Errorf("endpoint name %s is not unique, ensure all endpoint names are unique", endpoint.Name)
		}
		seenNames[endpoint.Name] = struct{}{}
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
	if strings.TrimSpace(endpoint.Name) == "" {
		return errors.New("endpoint name must be set")
	}
	return nil
}
