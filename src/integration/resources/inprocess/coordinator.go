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

	"github.com/cenkalti/backoff/v3"
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
)

const (
	retryMaxInterval = 5 * time.Second
	retryMaxTime     = time.Minute
)

// coordinator is an in-process implementation of resources.Coordinator for use
// in integration tests.
type coordinator struct {
	cfg      config.Configuration
	client   common.CoordinatorClient
	logger   *zap.Logger
	tmpDirs  []string
	embedded bool

	interruptCh chan<- error
	shutdownCh  <-chan struct{}
}

// CoordinatorOptions are options for starting a coordinator server.
type CoordinatorOptions struct {
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
// within the process. If you specify a port of 0 in any address, 0 will be automatically
// replace with an open port. This is similar to the behavior net.Listen provides for you.
// Similarly, for filepaths, if a "*" is specified in the config, then that field will
// be updated with a temp directory that will be cleaned up when the coordinator is destroyed.
// This should ensure that many of the same component can be spun up in-process without any
// issues with collisions.
func NewCoordinator(cfg config.Configuration, opts CoordinatorOptions) (resources.Coordinator, error) {
	// Replace any "0" ports with an open port
	cfg, err := updateCoordinatorPorts(cfg)
	if err != nil {
		return nil, err
	}

	// Replace any "*" filepath with a temporary directory
	cfg, tmpDirs, err := updateCoordinatorFilepaths(cfg)
	if err != nil {
		return nil, err
	}

	// Configure logger
	if opts.Logger == nil {
		opts.Logger, err = zap.NewDevelopment()
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
	coord := &coordinator{
		cfg: cfg,
		client: common.NewCoordinatorClient(common.CoordinatorClientOptions{
			Client:    &http.Client{},
			HTTPPort:  port,
			Logger:    opts.Logger,
			RetryFunc: retry,
		}),
		logger:  opts.Logger,
		tmpDirs: tmpDirs,
	}
	coord.start()

	return coord, nil
}

// NewEmbeddedCoordinator creates a coordinator from one embedded within an existing
// db node. This method expects that the DB node has already been started before
// being called.
func NewEmbeddedCoordinator(d *dbNode) (resources.Coordinator, error) {
	if d.started == false {
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

	return &coordinator{
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

func (c *coordinator) start() {
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

func (c *coordinator) GetNamespace() (admin.NamespaceGetResponse, error) {
	return c.client.GetNamespace()
}

func (c *coordinator) WaitForNamespace(name string) error {
	return c.client.WaitForNamespace(name)
}

func (c *coordinator) AddNamespace(request admin.NamespaceAddRequest) (admin.NamespaceGetResponse, error) {
	return c.client.AddNamespace(request)
}

func (c *coordinator) UpdateNamespace(request admin.NamespaceUpdateRequest) (admin.NamespaceGetResponse, error) {
	return c.client.UpdateNamespace(request)
}

func (c *coordinator) DeleteNamespace(namespaceID string) error {
	return c.client.DeleteNamespace(namespaceID)
}

func (c *coordinator) CreateDatabase(request admin.DatabaseCreateRequest) (admin.DatabaseCreateResponse, error) {
	return c.client.CreateDatabase(request)
}

func (c *coordinator) GetPlacement() (admin.PlacementGetResponse, error) {
	return c.client.GetPlacement()
}

func (c *coordinator) WaitForInstances(ids []string) error {
	return c.client.WaitForInstances(ids)
}

func (c *coordinator) WaitForShardsReady() error {
	return c.client.WaitForShardsReady()
}

func (c *coordinator) Close() error {
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

	if err := server.Close(server.CloseOptions{
		InterruptCh: c.interruptCh,
		ShutdownCh:  c.shutdownCh,
	}); err != nil {
		return err
	}

	return nil
}

func (c *coordinator) ApplyKVUpdate(update string) error {
	return c.client.ApplyKVUpdate(update)
}

func (c *coordinator) WriteCarbon(port int, metric string, v float64, t time.Time) error {
	return c.client.WriteCarbon(fmt.Sprintf("http://0.0.0.0/%d", port), metric, v, t)
}

func (c *coordinator) WriteProm(name string, tags map[string]string, samples []prompb.Sample) error {
	return c.client.WriteProm(name, tags, samples)
}

func (c *coordinator) RunQuery(
	verifier resources.ResponseVerifier,
	query string,
	headers map[string][]string,
) error {
	return c.client.RunQuery(verifier, query, headers)
}

func updateCoordinatorPorts(cfg config.Configuration) (config.Configuration, error) {
	if cfg.ListenAddress != nil {
		addr, _, _, err := nettest.MaybeGeneratePort(*cfg.ListenAddress)
		if err != nil {
			return cfg, err
		}

		cfg.ListenAddress = &addr
	}

	return cfg, nil
}

func updateCoordinatorFilepaths(cfg config.Configuration) (config.Configuration, []string, error) {
	tmpDirs := make([]string, 0, 1)

	for _, cluster := range cfg.Clusters {
		ec := cluster.Client.EnvironmentConfig
		if ec != nil {
			for _, svc := range ec.Services {
				if svc != nil && svc.Service != nil && svc.Service.CacheDir == "*" {
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

	return cfg, tmpDirs, nil
}

func retry(op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = retryMaxInterval
	bo.MaxElapsedTime = retryMaxTime
	return backoff.Retry(op, bo)
}
