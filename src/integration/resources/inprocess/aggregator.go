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
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	m3agg "github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/server"
	"github.com/m3db/m3/src/aggregator/tools/deploy"
	"github.com/m3db/m3/src/cmd/services/m3aggregator/config"
	"github.com/m3db/m3/src/integration/resources"
	nettest "github.com/m3db/m3/src/integration/resources/net"
	"github.com/m3db/m3/src/x/config/hostid"
	xos "github.com/m3db/m3/src/x/os"
)

var errAggregatorNotStarted = errors.New("aggregator instance has not started")

// Aggregator is an in-process implementation of resources.Aggregator for use
// in integration tests.
type Aggregator struct {
	cfg     config.Configuration
	logger  *zap.Logger
	tmpDirs []string
	startFn AggregatorStartFn

	started    bool
	httpClient deploy.AggregatorClient

	interruptCh chan<- error
	shutdownCh  <-chan struct{}
}

// AggregatorOptions are options of starting an in-process aggregator.
type AggregatorOptions struct {
	// Logger is the logger to use for the in-process aggregator.
	Logger *zap.Logger
	// StartFn is a custom function that can be used to start the Aggregator.
	StartFn AggregatorStartFn
	// Start indicates whether to start the aggregator instance
	Start bool
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
	hostID, err := cfg.AggregatorOrDefault().HostID.Resolve()
	if err != nil {
		return nil, err
	}

	loggingCfg := cfg.LoggingOrDefault()
	if len(loggingCfg.Fields) == 0 {
		loggingCfg.Fields = make(map[string]interface{})
	}
	loggingCfg.Fields["component"] = fmt.Sprintf("m3aggregator:%s", hostID)

	if opts.Logger == nil {
		var err error
		opts.Logger, err = resources.NewLogger()
		if err != nil {
			return nil, err
		}
	}

	agg := &Aggregator{
		cfg:        cfg,
		logger:     opts.Logger,
		tmpDirs:    tmpDirs,
		startFn:    opts.StartFn,
		started:    false,
		httpClient: deploy.NewAggregatorClient(&http.Client{}),
	}

	if opts.Start {
		agg.Start()
	}

	return agg, nil
}

// HostDetails returns the aggregator's host details.
func (a *Aggregator) HostDetails() (*resources.InstanceInfo, error) {
	id, err := a.cfg.AggregatorOrDefault().HostID.Resolve()
	if err != nil {
		return nil, err
	}

	addr, p, err := net.SplitHostPort(a.cfg.HTTPOrDefault().ListenAddress)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(p)
	if err != nil {
		return nil, err
	}

	m3msgAddr, m3msgP, err := net.SplitHostPort(a.cfg.M3MsgOrDefault().Server.ListenAddress)
	if err != nil {
		return nil, err
	}

	m3msgPort, err := strconv.Atoi(m3msgP)
	if err != nil {
		return nil, err
	}

	return &resources.InstanceInfo{
		ID:           id,
		Env:          a.cfg.KVClientOrDefault().Etcd.Env,
		Zone:         a.cfg.KVClientOrDefault().Etcd.Zone,
		Address:      addr,
		Port:         uint32(port),
		M3msgAddress: m3msgAddr,
		M3msgPort:    uint32(m3msgPort),
	}, nil
}

// Start starts the aggregator instance.
//nolint:dupl
func (a *Aggregator) Start() {
	if a.started {
		a.logger.Debug("aggregator instance has started already")
		return
	}
	a.started = true

	if a.startFn != nil {
		a.interruptCh, a.shutdownCh = a.startFn(&a.cfg)
		return
	}

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

// IsHealthy determines whether an instance is healthy.
func (a *Aggregator) IsHealthy() error {
	if !a.started {
		return errAggregatorNotStarted
	}

	return a.httpClient.IsHealthy(a.cfg.HTTPOrDefault().ListenAddress)
}

// Status returns the instance status.
func (a *Aggregator) Status() (m3agg.RuntimeStatus, error) {
	if !a.started {
		return m3agg.RuntimeStatus{}, errAggregatorNotStarted
	}

	return a.httpClient.Status(a.cfg.HTTPOrDefault().ListenAddress)
}

// Resign asks an aggregator instance to give up its current leader role if applicable.
func (a *Aggregator) Resign() error {
	if !a.started {
		return errAggregatorNotStarted
	}

	return a.httpClient.Resign(a.cfg.HTTPOrDefault().ListenAddress)
}

// Close closes the wrapper and releases any held resources, including
// deleting docker containers.
func (a *Aggregator) Close() error {
	if !a.started {
		return errAggregatorNotStarted
	}

	defer func() {
		for _, dir := range a.tmpDirs {
			if err := os.RemoveAll(dir); err != nil {
				a.logger.Error("error removing temp directory", zap.String("dir", dir), zap.Error(err))
			}
		}
		a.started = false
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

// Configuration returns a copy of the configuration used to
// start this aggregator.
func (a *Aggregator) Configuration() config.Configuration {
	return a.cfg
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
	aggCfg := cfg.AggregatorOrDefault()
	aggCfg.HostID = &hostid.Configuration{
		Resolver: hostid.ConfigResolver,
		Value:    &hostID,
	}
	cfg.Aggregator = &aggCfg

	return cfg
}

func updateAggregatorPorts(cfg config.Configuration) (config.Configuration, error) {
	httpCfg := cfg.HTTPOrDefault()
	addr, _, err := nettest.GeneratePort(httpCfg.ListenAddress)
	if err != nil {
		return cfg, err
	}
	httpCfg.ListenAddress = addr
	cfg.HTTP = &httpCfg

	metricsCfg := cfg.MetricsOrDefault()
	if metricsCfg.PrometheusReporter != nil && metricsCfg.PrometheusReporter.ListenAddress != "" {
		addr, _, err := nettest.GeneratePort(metricsCfg.PrometheusReporter.ListenAddress)
		if err != nil {
			return cfg, err
		}
		promReporter := *metricsCfg.PrometheusReporter
		promReporter.ListenAddress = addr
		metricsCfg.PrometheusReporter = &promReporter
	}
	cfg.Metrics = &metricsCfg

	m3msgCfg := cfg.M3MsgOrDefault()
	if m3msgAddr := m3msgCfg.Server.ListenAddress; m3msgAddr != "" {
		addr, _, err := nettest.GeneratePort(m3msgAddr)
		if err != nil {
			return cfg, err
		}
		m3msgCfg.Server.ListenAddress = addr
	}
	cfg.M3Msg = &m3msgCfg

	return cfg, nil
}

func updateAggregatorFilepaths(cfg config.Configuration) (config.Configuration, []string, error) {
	tmpDirs := make([]string, 0, 1)

	kvCfg := cfg.KVClientOrDefault()
	if kvCfg.Etcd != nil {
		dir, err := ioutil.TempDir("", "m3agg-*")
		if err != nil {
			return cfg, tmpDirs, err
		}
		tmpDirs = append(tmpDirs, dir)
		kvCfg.Etcd.CacheDir = dir
	}
	cfg.KVClient = &kvCfg

	return cfg, tmpDirs, nil
}
