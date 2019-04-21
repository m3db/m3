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

package client

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/topology"
	xtchannel "github.com/m3db/m3/src/dbnode/x/tchannel"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
)

var (
	errConfigurationMustSupplyConfig = errors.New(
		"must supply config when no topology initializer parameter supplied")
)

// Configuration is a configuration that can be used to construct a client.
type Configuration struct {
	// The environment (static or dynamic) configuration.
	EnvironmentConfig *environment.Configuration `yaml:"config"`

	// WriteConsistencyLevel specifies the write consistency level.
	WriteConsistencyLevel *topology.ConsistencyLevel `yaml:"writeConsistencyLevel"`

	// ReadConsistencyLevel specifies the read consistency level.
	ReadConsistencyLevel *topology.ReadConsistencyLevel `yaml:"readConsistencyLevel"`

	// ConnectConsistencyLevel specifies the cluster connect consistency level.
	ConnectConsistencyLevel *topology.ConnectConsistencyLevel `yaml:"connectConsistencyLevel"`

	// WriteTimeout is the write request timeout.
	WriteTimeout *time.Duration `yaml:"writeTimeout"`

	// FetchTimeout is the fetch request timeout.
	FetchTimeout *time.Duration `yaml:"fetchTimeout"`

	// ConnectTimeout is the cluster connect timeout.
	ConnectTimeout *time.Duration `yaml:"connectTimeout"`

	// WriteRetry is the write retry config.
	WriteRetry *retry.Configuration `yaml:"writeRetry"`

	// FetchRetry is the fetch retry config.
	FetchRetry *retry.Configuration `yaml:"fetchRetry"`

	// BackgroundHealthCheckFailLimit is the amount of times a background check
	// must fail before a connection is taken out of consideration.
	BackgroundHealthCheckFailLimit *int `yaml:"backgroundHealthCheckFailLimit"`

	// BackgroundHealthCheckFailThrottleFactor is the factor of the host connect
	// time to use when sleeping between a failed health check and the next check.
	BackgroundHealthCheckFailThrottleFactor *float64 `yaml:"backgroundHealthCheckFailThrottleFactor"`

	// HashingConfiguration is the configuration for hashing of IDs to shards.
	HashingConfiguration *HashingConfiguration `yaml:"hashing"`

	// Proto contains the configuration specific to running in the ProtoDataMode.
	Proto *ProtoConfiguration `yaml:"proto"`
}

// ProtoConfiguration is the configuration for running with ProtoDataMode enabled.
type ProtoConfiguration struct {
	SchemaFilePath string `yaml:"schemaFilePath"`
}

// Validate validates the ProtoConfiguration.
func (c *ProtoConfiguration) Validate() error {
	if c == nil {
		return nil
	}

	if c.SchemaFilePath == "" {
		return errors.New("schemaFilePath is required for Proto data mode")
	}

	return nil
}

// Validate validates the configuration.
func (c *Configuration) Validate() error {
	if c.WriteTimeout != nil && *c.WriteTimeout < 0 {
		return fmt.Errorf("m3db client writeTimeout was: %d but must be >= 0", *c.WriteTimeout)
	}

	if c.FetchTimeout != nil && *c.FetchTimeout < 0 {
		return fmt.Errorf("m3db client fetchTimeout was: %d but must be >= 0", *c.FetchTimeout)
	}

	if c.ConnectTimeout != nil && *c.ConnectTimeout < 0 {
		return fmt.Errorf("m3db client connectTimeout was: %d but must be >= 0", *c.ConnectTimeout)
	}

	if c.BackgroundHealthCheckFailLimit != nil &&
		(*c.BackgroundHealthCheckFailLimit < 0 || *c.BackgroundHealthCheckFailLimit > 10) {
		return fmt.Errorf(
			"m3db client backgroundHealthCheckFailLimit was: %d but must be >= 0 and <=10",
			*c.BackgroundHealthCheckFailLimit)
	}

	if c.BackgroundHealthCheckFailThrottleFactor != nil &&
		(*c.BackgroundHealthCheckFailThrottleFactor < 0 || *c.BackgroundHealthCheckFailThrottleFactor > 10) {
		return fmt.Errorf(
			"m3db client backgroundHealthCheckFailThrottleFactor was: %f but must be >= 0 and <=10",
			*c.BackgroundHealthCheckFailThrottleFactor)
	}

	if err := c.Proto.Validate(); err != nil {
		return fmt.Errorf("error validating M3DB client proto configuration: %v", err)
	}

	return nil
}

// HashingConfiguration is the configuration for hashing
type HashingConfiguration struct {
	// Murmur32 seed value
	Seed uint32 `yaml:"seed"`
}

// ConfigurationParameters are optional parameters that can be specified
// when creating a client from configuration, this is specified using
// a struct so that adding fields do not cause breaking changes to callers.
type ConfigurationParameters struct {
	// InstrumentOptions is a required argument when
	// constructing a client from configuration.
	InstrumentOptions instrument.Options

	// TopologyInitializer is an optional argument when
	// constructing a client from configuration.
	TopologyInitializer topology.Initializer

	// EncodingOptions is an optional argument when
	// constructing a client from configuration.
	EncodingOptions encoding.Options
}

// CustomOption is a programatic method for setting a client
// option after all the options have been set by configuration.
type CustomOption func(v Options) Options

// CustomAdminOption is a programatic method for setting a client
// admin option after all the options have been set by configuration.
type CustomAdminOption func(v AdminOptions) AdminOptions

// NewClient creates a new M3DB client using
// specified params and custom options.
func (c Configuration) NewClient(
	params ConfigurationParameters,
	custom ...CustomOption,
) (Client, error) {
	customAdmin := make([]CustomAdminOption, 0, len(custom))
	for _, opt := range custom {
		customAdmin = append(customAdmin, func(v AdminOptions) AdminOptions {
			return opt(Options(v)).(AdminOptions)
		})
	}

	v, err := c.NewAdminClient(params, customAdmin...)
	if err != nil {
		return nil, err
	}

	return v, err
}

// NewAdminClient creates a new M3DB admin client using
// specified params and custom options.
func (c Configuration) NewAdminClient(
	params ConfigurationParameters,
	custom ...CustomAdminOption,
) (AdminClient, error) {
	err := c.Validate()
	if err != nil {
		return nil, err
	}

	iopts := params.InstrumentOptions
	if iopts == nil {
		iopts = instrument.NewOptions()
	}

	writeRequestScope := iopts.MetricsScope().SubScope("write-req")
	fetchRequestScope := iopts.MetricsScope().SubScope("fetch-req")

	envCfg := environment.ConfigureResults{
		TopologyInitializer: params.TopologyInitializer,
	}

	if envCfg.TopologyInitializer == nil {
		if c.EnvironmentConfig.Service != nil {
			cfgParams := environment.ConfigurationParameters{
				InstrumentOpts: iopts,
			}
			if c.HashingConfiguration != nil {
				cfgParams.HashingSeed = c.HashingConfiguration.Seed
			}

			envCfg, err = c.EnvironmentConfig.Configure(cfgParams)
			if err != nil {
				err = fmt.Errorf("unable to create dynamic topology initializer, err: %v", err)
				return nil, err
			}
		} else if c.EnvironmentConfig.Static != nil {
			envCfg, err = c.EnvironmentConfig.Configure(environment.ConfigurationParameters{})

			if err != nil {
				err = fmt.Errorf("unable to create static topology initializer, err: %v", err)
				return nil, err
			}
		} else {
			return nil, errConfigurationMustSupplyConfig
		}
	}

	v := NewAdminOptions().
		SetTopologyInitializer(envCfg.TopologyInitializer).
		SetChannelOptions(xtchannel.NewDefaultChannelOptions()).
		SetInstrumentOptions(iopts)

	if c.WriteConsistencyLevel != nil {
		v = v.SetWriteConsistencyLevel(*c.WriteConsistencyLevel)
	}
	if c.ReadConsistencyLevel != nil {
		v = v.SetReadConsistencyLevel(*c.ReadConsistencyLevel)
	}
	if c.ConnectConsistencyLevel != nil {
		v.SetClusterConnectConsistencyLevel(*c.ConnectConsistencyLevel)
	}
	if c.BackgroundHealthCheckFailLimit != nil {
		v = v.SetBackgroundHealthCheckFailLimit(*c.BackgroundHealthCheckFailLimit)
	}
	if c.BackgroundHealthCheckFailThrottleFactor != nil {
		v = v.SetBackgroundHealthCheckFailThrottleFactor(*c.BackgroundHealthCheckFailThrottleFactor)
	}
	if c.WriteTimeout != nil {
		v = v.SetWriteRequestTimeout(*c.WriteTimeout)
	}
	if c.FetchTimeout != nil {
		v = v.SetFetchRequestTimeout(*c.FetchTimeout)
	}
	if c.ConnectTimeout != nil {
		v = v.SetClusterConnectTimeout(*c.ConnectTimeout)
	}
	if c.WriteRetry != nil {
		v = v.SetWriteRetrier(c.WriteRetry.NewRetrier(writeRequestScope))
	}
	if c.FetchRetry != nil {
		v = v.SetFetchRetrier(c.FetchRetry.NewRetrier(fetchRequestScope))
	}

	encodingOpts := params.EncodingOptions
	if encodingOpts == nil {
		encodingOpts = encoding.NewOptions()
	}

	v = v.SetReaderIteratorAllocate(func(r io.Reader) encoding.ReaderIterator {
		intOptimized := m3tsz.DefaultIntOptimizationEnabled
		return m3tsz.NewReaderIterator(r, intOptimized, encodingOpts)
	})

	if c.Proto != nil {
		v = v.SetEncodingProto(encodingOpts)
	}

	// Apply programtic custom options last
	opts := v.(AdminOptions)
	for _, opt := range custom {
		opts = opt(opts)
	}

	return NewAdminClient(opts)
}
