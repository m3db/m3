// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"github.com/m3db/m3/src/x/debug/config"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/log"
)

var (
	defaultMetricsSanitization        = instrument.PrometheusMetricSanitization
	defaultMetricsExtendedMetricsType = instrument.NoExtendedMetrics
	// copied from src/cmd/services.m3query/config/config.go
	defaultMetrics = instrument.MetricsConfiguration{
		RootScope: &instrument.ScopeConfiguration{
			Prefix: "aggregator",
		},
		PrometheusReporter: &instrument.PrometheusConfiguration{
			HandlerPath: "/metrics",
			// Default to coordinator (until https://github.com/m3db/m3/issues/682 is resolved)
			ListenAddress: "0.0.0.0:7204",
		},
		Sanitization:    &defaultMetricsSanitization,
		SamplingRate:    1.0,
		ExtendedMetrics: &defaultMetricsExtendedMetricsType,
	}
)

// Configuration contains top-level configuration.
type Configuration struct {
	// Logging configuration.
	Logging log.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics *instrument.MetricsConfiguration `yaml:"metrics"`

	// M3Msg server configuration.
	// Optional.
	M3Msg *M3MsgServerConfiguration `yaml:"m3msg"`

	// Raw TCP server configuration.
	// Optional.
	RawTCP *RawTCPServerConfiguration `yaml:"rawtcp"`

	// HTTP server configuration.
	// Optional.
	HTTP *HTTPServerConfiguration `yaml:"http"`

	// Client configuration for key value store.
	KVClient KVClientConfiguration `yaml:"kvClient" validate:"nonzero"`

	// Runtime options configuration.
	RuntimeOptions RuntimeOptionsConfiguration `yaml:"runtimeOptions"`

	// Aggregator configuration.
	Aggregator AggregatorConfiguration `yaml:"aggregator"`

	// Debug configuration.
	Debug config.DebugConfiguration `yaml:"debug"`
}

// MetricsOrDefault returns metrics configuration or defaults.
func (c *Configuration) MetricsOrDefault() *instrument.MetricsConfiguration {
	if c.Metrics == nil {
		return &defaultMetrics
	}

	return c.Metrics
}
