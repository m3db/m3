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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
)

func TestNewFromConfiguration(t *testing.T) {
	opts, err := NewOptions(&config.PrometheusRemoteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteBackendEndpointConfiguration{{
			Name:    "testEndpoint",
			Address: "testAddress",
			StoragePolicy: &config.PrometheusRemoteBackendStoragePolicyConfiguration{
				Resolution: time.Second,
				Retention:  time.Millisecond,
			},
		}},
		RequestTimeout:  ptrDuration(time.Nanosecond),
		ConnectTimeout:  ptrDuration(time.Microsecond),
		KeepAlive:       ptrDuration(time.Millisecond),
		IdleConnTimeout: ptrDuration(time.Second),
		MaxIdleConns:    ptrInt(1),
	}, tally.NoopScope)
	require.NoError(t, err)

	assert.Equal(t, []EndpointOptions{{
		name:       "testEndpoint",
		address:    "testAddress",
		resolution: time.Second,
		retention:  time.Millisecond,
	}}, opts.endpoints)
	assert.Equal(t, tally.NoopScope, opts.scope)
	assert.Equal(t, time.Nanosecond, opts.httpOptions.RequestTimeout)
	assert.Equal(t, time.Microsecond, opts.httpOptions.ConnectTimeout)
	assert.Equal(t, time.Millisecond, opts.httpOptions.KeepAlive)
	assert.Equal(t, time.Second, opts.httpOptions.IdleConnTimeout)
	assert.Equal(t, 1, opts.httpOptions.MaxIdleConns)
	assert.Equal(t, true, opts.httpOptions.DisableCompression)
}

func TestHTTPDefaults(t *testing.T) {
	cfg, err := NewOptions(&config.PrometheusRemoteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteBackendEndpointConfiguration{getValidEndpointConfiguration()},
	}, tally.NoopScope)
	require.NoError(t, err)
	opts := cfg.httpOptions

	assert.Equal(t, 60*time.Second, opts.RequestTimeout)
	assert.Equal(t, 5*time.Second, opts.ConnectTimeout)
	assert.Equal(t, 60*time.Second, opts.KeepAlive)
	assert.Equal(t, 60*time.Second, opts.IdleConnTimeout)
	assert.Equal(t, 100, opts.MaxIdleConns)
	assert.Equal(t, true, opts.DisableCompression)
}

func TestValidation(t *testing.T) {
	t.Run("can't be nil", func(t *testing.T) {
		assertValidationError(t, nil, "prometheusRemoteBackend configuration is required")
	})

	t.Run("at least 1 endpoint", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.Endpoints = nil
		assertValidationError(t, &cfg, "at least one endpoint must be configured when using prom-remote backend type")
	})

	t.Run("valid endpoint", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.Endpoints[0].Address = ""
		assertValidationError(t, &cfg, "endpoint address must be set")
	})

	t.Run("name required for endpoint", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.Endpoints[0].Name = ""
		assertValidationError(t, &cfg, "endpoint name must be set")
		cfg.Endpoints[0].Name = "    "
		assertValidationError(t, &cfg, "endpoint name must be set")
	})

	t.Run("name must be unique", func(t *testing.T) {
		cfg := getValidConfig()
		endpoint := getValidEndpointConfiguration()
		cfg.Endpoints = []config.PrometheusRemoteBackendEndpointConfiguration{endpoint, endpoint}
		assertValidationError(t, &cfg, "endpoint name testName is not unique, ensure all endpoint names are unique")
	})

	t.Run("non negative keep alive", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.KeepAlive = ptrDuration(-1)
		assertValidationError(t, &cfg, "keepAlive can't be negative")
	})
	t.Run("non negative max idle conns", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.MaxIdleConns = ptrInt(-1)
		assertValidationError(t, &cfg, "maxIdleConns can't be negative")
	})

	t.Run("non negative idle conn timeout", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.IdleConnTimeout = ptrDuration(-1)
		assertValidationError(t, &cfg, "idleConnTimeout can't be negative")
	})

	t.Run("non negative request timeout", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.RequestTimeout = ptrDuration(-1)
		assertValidationError(t, &cfg, "requestTimeout can't be negative")
	})

	t.Run("non negative connect timeout", func(t *testing.T) {
		cfg := getValidConfig()
		cfg.ConnectTimeout = ptrDuration(-1)
		assertValidationError(t, &cfg, "connectTimeout can't be negative")
	})
}

func TestValidateEndpoint(t *testing.T) {
	t.Run("address required", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.Address = ""
		assertEndpointValidationError(t, cfg, "endpoint address must be set")
	})

	t.Run("address spaces trimmed", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.Address = "    "
		assertEndpointValidationError(t, cfg, "endpoint address must be set")
	})

	t.Run("storage policy is optional", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.StoragePolicy = nil
		err := validateEndpointConfiguration(cfg)
		require.NoError(t, err)
	})

	t.Run("retention must be positive", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.StoragePolicy.Retention = -1
		assertEndpointValidationError(t, cfg, "endpoint retention must be positive")

		cfg.StoragePolicy.Retention = 0
		assertEndpointValidationError(t, cfg, "endpoint retention must be positive")
	})

	t.Run("resolution must be positive", func(t *testing.T) {
		cfg := getValidEndpointConfiguration()
		cfg.StoragePolicy.Resolution = -1
		assertEndpointValidationError(t, cfg, "endpoint resolution must be positive")

		cfg.StoragePolicy.Resolution = 0
		assertEndpointValidationError(t, cfg, "endpoint resolution must be positive")
	})
}

func assertValidationError(t *testing.T, cfg *config.PrometheusRemoteBackendConfiguration, expectedMsg string) {
	_, err := NewOptions(cfg, tally.NoopScope)
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedMsg)
}

func assertEndpointValidationError(
	t *testing.T,
	cfg config.PrometheusRemoteBackendEndpointConfiguration,
	expectedMsg string,
) {
	err := validateEndpointConfiguration(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), expectedMsg)
}

func getValidConfig() config.PrometheusRemoteBackendConfiguration {
	return config.PrometheusRemoteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteBackendEndpointConfiguration{getValidEndpointConfiguration()},
	}
}

func getValidEndpointConfiguration() config.PrometheusRemoteBackendEndpointConfiguration {
	return config.PrometheusRemoteBackendEndpointConfiguration{
		Name:    "testName",
		Address: "testAddress",
		StoragePolicy: &config.PrometheusRemoteBackendStoragePolicyConfiguration{
			Retention:  time.Second,
			Resolution: time.Second,
		},
	}
}

func ptrDuration(n time.Duration) *time.Duration { return &n }

func ptrInt(n int) *int { return &n }
