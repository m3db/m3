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

// Package inprocess contains code for spinning up M3 resources in-process for
// the sake of integration testing.
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

	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/common"
	nettest "github.com/m3db/m3/src/integration/resources/net"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/server"
	xconfig "github.com/m3db/m3/src/x/config"
	xos "github.com/m3db/m3/src/x/os"
)

const (
	interruptTimeout = 5 * time.Second
	shutdownTimeout  = time.Minute
)

// Coordinator is an in-process implementation of resources.Coordinator for use
// in integration tests.
type Coordinator struct {
	cfg      config.Configuration
	client   common.CoordinatorClient
	logger   *zap.Logger
	tmpDirs  []string
	embedded bool
	startFn  StartFn

	interruptCh chan<- error
	shutdownCh  <-chan struct{}
}

// CoordinatorOptions are options for starting a coordinator server.
type CoordinatorOptions struct {
	// GeneratePorts will automatically update the config to use open ports
	// if set to true. If false, configuration is used as-is re: ports.
	GeneratePorts bool
	// StartFn is a custom function that can be used to start the Coordinator.
	StartFn StartFn
	// Logger is the logger to use for the coordinator. If not provided,
	// a default one will be created.
	Logger *zap.Logger
}

// NewCoordinatorFromConfigFile creates a new in-process coordinator based on the config file
// and options provided.
func NewCoordinatorFromConfigFile(pathToCfg string, opts CoordinatorOptions) (resources.Coordinator, error) {
	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, pathToCfg, xconfig.Options{}); err != nil {
		return nil, err
	}

	return NewCoordinator(cfg, opts)
}

// NewCoordinatorFromYAML creates a new in-process coordinator based on the YAML configuration string
// and options provided.
func NewCoordinatorFromYAML(yamlCfg string, opts CoordinatorOptions) (resources.Coordinator, error) {
	var cfg config.Configuration
	if err := yaml.Unmarshal([]byte(yamlCfg), &cfg); err != nil {
		return nil, err
	}

	return NewCoordinator(cfg, opts)
}

// NewCoordinator creates a new in-process coordinator based on the configuration
// and options provided. Use NewCoordinator or any of the convenience constructors
// (e.g. NewCoordinatorFromYAML, NewCoordinatorFromConfigFile) to get a running
// coordinator.
//
// The most typical usage of this method will be in an integration test to validate
// some behavior. For example, assuming we have a running DB node already, we could
// do the following to create a new namespace and write to it (note: ignoring error checking):
//
//    coord, _ := NewCoordinatorFromYAML(defaultCoordConfig, CoordinatorOptions{})
//    coord.AddNamespace(admin.NamespaceAddRequest{...})
//    coord.WaitForNamespace(namespaceName)
//    coord.WriteProm("cpu", map[string]string{"host", host}, samples)
//
// The coordinator will start up as you specify in your config. However, there is some
// helper logic to avoid port and filesystem collisions when spinning up multiple components
// within the process. If you specify a GeneratePorts: true in the CoordinatorOptions, address ports
// will be replaced with an open port.
//
// Similarly, filepath fields will  be updated with a temp directory that will be cleaned up
// when the coordinator is destroyed. This should ensure that many of the same component can be
// spun up in-process without any issues with collisions.
func NewCoordinator(cfg config.Configuration, opts CoordinatorOptions) (resources.Coordinator, error) {
	// Massage config so it runs properly in tests.
	cfg, tmpDirs, err := updateCoordinatorConfig(cfg, opts)
	if err != nil {
		return nil, err
	}

	logging := cfg.LoggingOrDefault()
	if len(logging.Fields) == 0 {
		logging.Fields = make(map[string]interface{})
	}
	logging.Fields["component"] = "coordinator"
	cfg.Logging = &logging

	// Configure logger
	if opts.Logger == nil {
		opts.Logger, err = NewLogger()
		if err != nil {
			return nil, err
		}
	}

	// Get HTTP port
	_, p, err := net.SplitHostPort(cfg.ListenAddressOrDefault())
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(p)
	if err != nil {
		return nil, err
	}

	// Start the coordinator
	coord := &Coordinator{
		cfg: cfg,
		client: common.NewCoordinatorClient(common.CoordinatorClientOptions{
			Client:    &http.Client{},
			HTTPPort:  port,
			Logger:    opts.Logger,
			RetryFunc: retry,
		}),
		logger:  opts.Logger,
		tmpDirs: tmpDirs,
		startFn: opts.StartFn,
	}
	coord.start()

	return coord, nil
}

// NewEmbeddedCoordinator creates a coordinator from one embedded within an existing
// db node. This method expects that the DB node has already been started before
// being called.
func NewEmbeddedCoordinator(d *DBNode) (resources.Coordinator, error) {
	if !d.started {
		return nil, errors.New("dbnode must be started to create the embedded coordinator")
	}

	_, p, err := net.SplitHostPort(d.cfg.Coordinator.ListenAddressOrDefault())
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(p)
	if err != nil {
		return nil, err
	}

	return &Coordinator{
		cfg: *d.cfg.Coordinator,
		client: common.NewCoordinatorClient(common.CoordinatorClientOptions{
			Client:    &http.Client{},
			HTTPPort:  port,
			Logger:    d.logger,
			RetryFunc: retry,
		}),
		embedded:    true,
		logger:      d.logger,
		interruptCh: d.interruptCh,
		shutdownCh:  d.shutdownCh,
	}, nil
}

func (c *Coordinator) start() {
	if c.startFn != nil {
		c.interruptCh, c.shutdownCh = c.startFn()
		return
	}

	interruptCh := make(chan error, 1)
	shutdownCh := make(chan struct{}, 1)

	go func() {
		server.Run(server.RunOptions{
			Config:      c.cfg,
			InterruptCh: interruptCh,
			ShutdownCh:  shutdownCh,
		})
	}()

	c.interruptCh = interruptCh
	c.shutdownCh = shutdownCh
}

// GetNamespace gets namespaces.
func (c *Coordinator) GetNamespace() (admin.NamespaceGetResponse, error) {
	return c.client.GetNamespace()
}

// WaitForNamespace blocks until the given namespace is enabled.
func (c *Coordinator) WaitForNamespace(name string) error {
	return c.client.WaitForNamespace(name)
}

// AddNamespace adds a namespace.
func (c *Coordinator) AddNamespace(request admin.NamespaceAddRequest) (admin.NamespaceGetResponse, error) {
	return c.client.AddNamespace(request)
}

// UpdateNamespace updates the namespace.
func (c *Coordinator) UpdateNamespace(request admin.NamespaceUpdateRequest) (admin.NamespaceGetResponse, error) {
	return c.client.UpdateNamespace(request)
}

// DeleteNamespace removes the namespace.
func (c *Coordinator) DeleteNamespace(namespaceID string) error {
	return c.client.DeleteNamespace(namespaceID)
}

// CreateDatabase creates a database.
func (c *Coordinator) CreateDatabase(request admin.DatabaseCreateRequest) (admin.DatabaseCreateResponse, error) {
	return c.client.CreateDatabase(request)
}

// GetPlacement gets placements.
func (c *Coordinator) GetPlacement(
	opts resources.PlacementRequestOptions,
) (admin.PlacementGetResponse, error) {
	return c.client.GetPlacement(opts)
}

// InitPlacement initializes placements.
func (c *Coordinator) InitPlacement(
	opts resources.PlacementRequestOptions,
	req admin.PlacementInitRequest,
) (admin.PlacementGetResponse, error) {
	return c.client.InitPlacement(opts, req)
}

// WaitForInstances blocks until the given instance is available.
func (c *Coordinator) WaitForInstances(ids []string) error {
	return c.client.WaitForInstances(ids)
}

// WaitForShardsReady waits until all shards gets ready.
func (c *Coordinator) WaitForShardsReady() error {
	return c.client.WaitForShardsReady()
}

// Close closes the wrapper and releases any held resources, including
// deleting docker containers.
func (c *Coordinator) Close() error {
	if c.embedded {
		// NB(nate): for embedded coordinators, close is handled by the dbnode that
		// it is spun up inside of.
		return nil
	}

	defer func() {
		for _, dir := range c.tmpDirs {
			if err := os.RemoveAll(dir); err != nil {
				c.logger.Error("error removing temp directory", zap.String("dir", dir), zap.Error(err))
			}
		}
	}()

	select {
	case c.interruptCh <- xos.NewInterruptError("in-process coordinator being shut down"):
	case <-time.After(interruptTimeout):
		return errors.New("timeout sending interrupt. closing without graceful shutdown")
	}

	select {
	case <-c.shutdownCh:
	case <-time.After(shutdownTimeout):
		return errors.New("timeout waiting for shutdown notification. coordinator closing may" +
			" not be completely graceful")
	}

	return nil
}

// InitM3msgTopic initializes an m3msg topic.
func (c *Coordinator) InitM3msgTopic(
	opts resources.M3msgTopicOptions,
	req admin.TopicInitRequest,
) (admin.TopicGetResponse, error) {
	return c.client.InitM3msgTopic(opts, req)
}

// GetM3msgTopic gets an m3msg topic.
func (c *Coordinator) GetM3msgTopic(
	opts resources.M3msgTopicOptions,
) (admin.TopicGetResponse, error) {
	return c.client.GetM3msgTopic(opts)
}

// AddM3msgTopicConsumer adds a consumer service to an m3msg topic.
func (c *Coordinator) AddM3msgTopicConsumer(
	opts resources.M3msgTopicOptions,
	req admin.TopicAddRequest,
) (admin.TopicGetResponse, error) {
	return c.client.AddM3msgTopicConsumer(opts, req)
}

// ApplyKVUpdate applies a KV update.
func (c *Coordinator) ApplyKVUpdate(update string) error {
	return c.client.ApplyKVUpdate(update)
}

// WriteCarbon writes a carbon metric datapoint at a given time.
func (c *Coordinator) WriteCarbon(port int, metric string, v float64, t time.Time) error {
	return c.client.WriteCarbon(fmt.Sprintf("http://0.0.0.0/%d", port), metric, v, t)
}

// WriteProm writes a prometheus metric.
func (c *Coordinator) WriteProm(name string, tags map[string]string, samples []prompb.Sample) error {
	return c.client.WriteProm(name, tags, samples)
}

// RunQuery runs the given query with a given verification function.
func (c *Coordinator) RunQuery(
	verifier resources.ResponseVerifier,
	query string,
	headers map[string][]string,
) error {
	return c.client.RunQuery(verifier, query, headers)
}

func updateCoordinatorConfig(
	cfg config.Configuration,
	opts CoordinatorOptions,
) (config.Configuration, []string, error) {
	var (
		tmpDirs []string
		err     error
	)
	if opts.GeneratePorts {
		// Replace any port with an open port
		cfg, err = updateCoordinatorPorts(cfg)
		if err != nil {
			return config.Configuration{}, nil, err
		}
	}

	// Replace any filepath with a temporary directory
	cfg, tmpDirs, err = updateCoordinatorFilepaths(cfg)
	if err != nil {
		return config.Configuration{}, nil, err
	}

	return cfg, tmpDirs, nil
}

func updateCoordinatorPorts(cfg config.Configuration) (config.Configuration, error) {
	addr, _, err := nettest.GeneratePort(cfg.ListenAddressOrDefault())
	if err != nil {
		return cfg, err
	}
	cfg.ListenAddress = &addr

	metrics := cfg.MetricsOrDefault()
	if metrics.PrometheusReporter != nil && metrics.PrometheusReporter.ListenAddress != "" {
		addr, _, err := nettest.GeneratePort(metrics.PrometheusReporter.ListenAddress)
		if err != nil {
			return cfg, err
		}
		metrics.PrometheusReporter.ListenAddress = addr
	}
	cfg.Metrics = metrics

	if cfg.RPC != nil && cfg.RPC.ListenAddress != "" {
		addr, _, err := nettest.GeneratePort(cfg.RPC.ListenAddress)
		if err != nil {
			return cfg, err
		}
		cfg.RPC.ListenAddress = addr
	}

	if cfg.Ingest != nil && cfg.Ingest.M3Msg.Server.ListenAddress != "" {
		addr, _, err := nettest.GeneratePort(cfg.Ingest.M3Msg.Server.ListenAddress)
		if err != nil {
			return cfg, err
		}
		cfg.Ingest.M3Msg.Server.ListenAddress = addr
	}

	if cfg.Carbon != nil && cfg.Carbon.Ingester != nil {
		addr, _, err := nettest.GeneratePort(cfg.Carbon.Ingester.ListenAddressOrDefault())
		if err != nil {
			return cfg, err
		}
		cfg.Carbon.Ingester.ListenAddress = addr
	}

	return cfg, nil
}

func updateCoordinatorFilepaths(cfg config.Configuration) (config.Configuration, []string, error) {
	tmpDirs := make([]string, 0, 1)

	for _, cluster := range cfg.Clusters {
		ec := cluster.Client.EnvironmentConfig
		if ec != nil {
			for _, svc := range ec.Services {
				if svc != nil && svc.Service != nil {
					dir, err := ioutil.TempDir("", "m3kv-*")
					if err != nil {
						return cfg, tmpDirs, err
					}

					tmpDirs = append(tmpDirs, dir)
					svc.Service.CacheDir = dir
				}
			}
		}
	}

	if cfg.ClusterManagement.Etcd != nil {
		dir, err := ioutil.TempDir("", "m3kv-*")
		if err != nil {
			return cfg, tmpDirs, err
		}

		tmpDirs = append(tmpDirs, dir)
		cfg.ClusterManagement.Etcd.CacheDir = dir
	}

	return cfg, tmpDirs, nil
}
