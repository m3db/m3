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

	"gopkg.in/yaml.v2"
)

// Configuration contains top-level configuration.
type Configuration struct {
	// Logging configuration.
	Logging *log.Configuration `yaml:"logging"`

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
	KVClient *KVClientConfiguration `yaml:"kvClient" validate:"nonzero"`

	// Runtime options configuration.
	RuntimeOptions *RuntimeOptionsConfiguration `yaml:"runtimeOptions"`

	// Aggregator configuration.
	Aggregator *AggregatorConfiguration `yaml:"aggregator"`

	// Debug configuration.
	Debug config.DebugConfiguration `yaml:"debug"`
}

// LoggingOrDefault returns the logging configuration or defaults.
func (c *Configuration) LoggingOrDefault() log.Configuration {
	if c.Logging != nil {
		return *c.Logging
	}

	return defaultLogging
}

// MetricsOrDefault returns the metrics config or default.
func (c *Configuration) MetricsOrDefault() instrument.MetricsConfiguration {
	if c.Metrics != nil {
		return *c.Metrics
	}

	return defaultMetrics
}

// M3MsgOrDefault returns the m3msg config or default.
func (c *Configuration) M3MsgOrDefault() M3MsgServerConfiguration {
	if c.M3Msg != nil {
		return *c.M3Msg
	}

	return defaultM3Msg
}

// HTTPOrDefault returns the http config or default.
func (c *Configuration) HTTPOrDefault() HTTPServerConfiguration {
	if c.HTTP != nil {
		return *c.HTTP
	}

	return defaultHTTP
}

// KVClientOrDefault returns the kv client or default.
func (c *Configuration) KVClientOrDefault() KVClientConfiguration {
	if c.KVClient != nil {
		return *c.KVClient
	}

	return defaultKV
}

// RuntimeOptionsOrDefault returns the runtime options or default.
func (c *Configuration) RuntimeOptionsOrDefault() RuntimeOptionsConfiguration {
	if c.RuntimeOptions != nil {
		return *c.RuntimeOptions
	}

	return defaultRuntimeOptions
}

// AggregatorOrDefault returns the aggregator config or default.
func (c *Configuration) AggregatorOrDefault() AggregatorConfiguration {
	if c.Aggregator != nil {
		return *c.Aggregator
	}

	return defaultAggregator
}

// DeepCopy returns a deep copy of the current configuration object.
func (c *Configuration) DeepCopy() (Configuration, error) {
	rawCfg, err := yaml.Marshal(c)
	if err != nil {
		return Configuration{}, err
	}
	var dupe Configuration
	if err := yaml.Unmarshal(rawCfg, &dupe); err != nil {
		return Configuration{}, err
	}
	return dupe, nil
}
