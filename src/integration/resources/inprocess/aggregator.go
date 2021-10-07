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

package inprocess

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	m3agg "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/server"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	"github.com/m3db/m3/src/integration/resources"
	nettest "github.com/m3db/m3/src/integration/resources/net"
	"github.com/m3db/m3/src/x/config/hostid"
	xos "github.com/m3db/m3/src/x/os"
)

type aggregator struct {
	cfg     config.Configuration
	logger  *zap.Logger
	tmpDirs []string

	interruptCh chan<- error
	shutdownCh  <-chan struct{}
}

// AggregatorOptions are options of starting an in-process aggregator.
type AggregatorOptions struct {
	// Logger is the logger to use for the in-process aggregator.
	Logger *zap.Logger

	// GeneratePorts will automatically update the config to use open ports
	// if set to true. If false, configuration is used as-is re: ports.
	GeneratePorts bool

	// GenerateHostID will automatically update the host ID specified in
	// the config if set to true. If false, configuration is used as-is re: host ID.
	GenerateHostID bool
}

// NewAggregatorFromYAML creates a new in-process aggregator based on the yaml configuration
// and options provided.
func NewAggregatorFromYAML(yamlCfg string, opts AggregatorOptions) (resources.Aggregator, error) {
	var cfg config.Configuration
	if err := yaml.Unmarshal([]byte(yamlCfg), &cfg); err != nil {
		return nil, err
	}

	return NewAggregator(cfg, opts)
}

// NewAggregator creates a new in-process aggregator based on the configuration
// and options provided.
func NewAggregator(cfg config.Configuration, opts AggregatorOptions) (resources.Aggregator, error) {
	cfg, tmpDirs, err := updateAggregatorConfig(cfg, opts)
	if err != nil {
		return nil, err
	}

	// configure logger
	hostID, err := cfg.Aggregator.HostIDOrDefault().Resolve()
	if err != nil {
		return nil, err
	}

	if len(cfg.Logging.Fields) == 0 {
		cfg.Logging.Fields = make(map[string]interface{})
	}
	cfg.Logging.Fields["component"] = fmt.Sprintf("m3aggregator:%s", hostID)

	if opts.Logger == nil {
		var err error
		opts.Logger, err = newLogger()
		if err != nil {
			return nil, err
		}
	}

	agg := &aggregator{
		cfg:     cfg,
		logger:  opts.Logger,
		tmpDirs: tmpDirs,
	}
	agg.start()

	return agg, nil
}

func (a *aggregator) IsHealthy(instance string) error {
	return nil
}

func (a *aggregator) Status(instance string) (m3agg.RuntimeStatus, error) {
	return m3agg.RuntimeStatus{}, nil
}

func (a *aggregator) Resign(instance string) error {
	return nil
}

func (a *aggregator) Close() error {
	defer func() {
		for _, dir := range a.tmpDirs {
			if err := os.RemoveAll(dir); err != nil {
				a.logger.Error("error removing temp directory", zap.String("dir", dir), zap.Error(err))
			}
		}
	}()

	select {
	case a.interruptCh <- xos.NewInterruptError("in-process aggregator being shut down"):
	case <-time.After(interruptTimeout):
		return errors.New("timeout sending interrupt. closing without graceful shutdown")
	}

	select {
	case <-a.shutdownCh:
	case <-time.After(shutdownTimeout):
		return errors.New("timeout waiting for shutdown notification. server closing may" +
			" not be completely graceful")
	}

	return nil
}

func (a *aggregator) start() {
	interruptCh := make(chan error, 1)
	shutdownCh := make(chan struct{}, 1)

	go func() {
		server.Run(server.RunOptions{
			Config:      a.cfg,
			InterruptCh: interruptCh,
			ShutdownCh:  shutdownCh,
		})
	}()

	a.interruptCh = interruptCh
	a.shutdownCh = shutdownCh
}

func updateAggregatorConfig(
	cfg config.Configuration,
	opts AggregatorOptions,
) (config.Configuration, []string, error) {
	var (
		tmpDirs []string
		err     error
	)

	// Replace host ID with a config-based version.
	if opts.GenerateHostID {
		cfg = updateAggregatorHostID(cfg)
	}

	// Replace any ports with open ports
	if opts.GeneratePorts {
		cfg, err = updateAggregatorPorts(cfg)
		if err != nil {
			return config.Configuration{}, nil, err
		}
	}

	// Replace any filepath with a temporary directory
	cfg, tmpDirs, err = updateAggregatorFilepaths(cfg)
	if err != nil {
		return config.Configuration{}, nil, err
	}

	return cfg, tmpDirs, nil
}

func updateAggregatorHostID(cfg config.Configuration) config.Configuration {
	hostID := uuid.New().String()
	cfg.Aggregator.HostID = &hostid.Configuration{
		Resolver: hostid.ConfigResolver,
		Value:    &hostID,
	}

	return cfg
}

func updateAggregatorPorts(cfg config.Configuration) (config.Configuration, error) {
	if cfg.HTTP != nil && len(cfg.HTTP.ListenAddress) > 0 {
		addr, _, err := nettest.GeneratePort(cfg.HTTP.ListenAddress)
		if err != nil {
			return cfg, err
		}

		cfg.HTTP.ListenAddress = addr
	}

	if cfg.Metrics.PrometheusReporter != nil && len(cfg.Metrics.PrometheusReporter.ListenAddress) > 0 {
		addr, _, err := nettest.GeneratePort(cfg.Metrics.PrometheusReporter.ListenAddress)
		if err != nil {
			return cfg, err
		}

		cfg.Metrics.PrometheusReporter.ListenAddress = addr
	}

	return cfg, nil
}

func updateAggregatorFilepaths(cfg config.Configuration) (config.Configuration, []string, error) {
	tmpDirs := make([]string, 0, 1)
	if cfg.KVClient.Etcd != nil && cfg.KVClient.Etcd.CacheDir == "*" {
		dir, err := ioutil.TempDir("", "m3agg-*")
		if err != nil {
			return cfg, tmpDirs, err
		}
		tmpDirs = append(tmpDirs, dir)
		cfg.KVClient.Etcd.CacheDir = dir
	}

	return cfg, tmpDirs, nil
}
