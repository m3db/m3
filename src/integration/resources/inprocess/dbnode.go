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
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"time"

	"github.com/cenkalti/backoff/v3"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/server"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration"
	"github.com/m3db/m3/src/integration/resources"
	nettest "github.com/m3db/m3/src/integration/resources/net"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	xconfig "github.com/m3db/m3/src/x/config"
)

// TODO(nate): make configurable
const defaultRPCTimeout = time.Minute

type dbNode struct {
	cfg     config.Configuration
	logger  *zap.Logger
	tmpDirs []string

	interruptCh chan<- error
	shutdownCh  <-chan struct{}
	tchanClient *integration.TestTChannelClient
}

// DBNodeOptions are options for starting a DB node server.
type DBNodeOptions struct {
	// Logger is the logger to use for the dbnode. If not provided,
	// a default one will be created.
	Logger *zap.Logger
}

// NewDBNodeFromConfigFile creates a new in-process DB node based on the config file
// and options provided.
func NewDBNodeFromConfigFile(pathToCfg string, opts DBNodeOptions) (resources.Node, error) {
	var cfg config.Configuration
	if err := xconfig.LoadFile(&cfg, pathToCfg, xconfig.Options{}); err != nil {
		return nil, err
	}

	return NewDBNode(cfg, opts)
}

// NewDBNodeFromYAML creates a new in-process DB node based on the YAML configuration string
// and options provided.
func NewDBNodeFromYAML(yamlCfg string, opts DBNodeOptions) (resources.Node, error) {
	var cfg config.Configuration
	if err := yaml.Unmarshal([]byte(yamlCfg), &cfg); err != nil {
		return nil, err
	}

	return NewDBNode(cfg, opts)
}

// NewDBNode creates a new in-process DB node based on the configuration
// and options provided.
func NewDBNode(cfg config.Configuration, opts DBNodeOptions) (resources.Node, error) {
	// Replace any "0" ports with an open port
	cfg, err := updateDBNodePorts(cfg)
	if err != nil {
		return nil, err
	}

	// Replace any "*" filepath with a temporary directory
	cfg, tmpDirs, err := updateDBNodeFilepaths(cfg)
	if err != nil {
		return nil, err
	}

	// Configure TChannel client for hitting the DB node.
	tchanClient, err := integration.NewTChannelClient("client", cfg.DB.ListenAddressOrDefault())
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

	// Start the DB node
	node := &dbNode{
		cfg:         cfg,
		logger:      opts.Logger,
		tchanClient: tchanClient,
		tmpDirs:     tmpDirs,
	}
	node.start()

	return node, nil
}

func (d *dbNode) start() {
	interruptCh := make(chan error, d.cfg.Components())
	shutdownCh := make(chan struct{}, d.cfg.Components())
	go func() {
		server.RunComponents(server.Options{
			Configuration: d.cfg,
			InterruptCh:   interruptCh,
			ShutdownCh:    shutdownCh,
		})
	}()

	d.interruptCh = interruptCh
	d.shutdownCh = shutdownCh
}

func (d *dbNode) HostDetails(port int) (*admin.Host, error) {
	// TODO(nate): implement once working on helpers for spinning up
	// a multi-node cluster since that's what it's currently being
	// used for based on the docker-based implementation.
	// NOTES:
	// - id we can generate
	// - isolation_group set a constant?
	// - weight just use 1024?
	// - address is 0.0.0.0
	// - port is the listen address (get from config)

	return &admin.Host{
		Id:             "foo",
		IsolationGroup: "foo-a",
		Zone:           "embedded",
		Weight:         1024,
		Address:        "0.0.0.0",
		Port:           uint32(port),
	}, nil
}

func (d *dbNode) Health() (*rpc.NodeHealthResult_, error) {
	return d.tchanClient.TChannelClientHealth(defaultRPCTimeout)
}

func (d *dbNode) WaitForBootstrap() error {
	return retry(func() error {
		health, err := d.Health()
		if err != nil {
			return err
		}

		if !health.GetBootstrapped() {
			err = fmt.Errorf("not bootstrapped")
			d.logger.Error("node not bootstrapped", zap.Error(err))
			return err
		}

		return nil
	})
}

func (d *dbNode) WritePoint(req *rpc.WriteRequest) error {
	return d.tchanClient.TChannelClientWrite(defaultRPCTimeout, req)
}

func (d *dbNode) WriteTaggedPoint(req *rpc.WriteTaggedRequest) error {
	return d.tchanClient.TChannelClientWriteTagged(defaultRPCTimeout, req)
}

func (d *dbNode) AggregateTiles(req *rpc.AggregateTilesRequest) (int64, error) {
	res, err := d.tchanClient.TChannelClientAggregateTiles(defaultRPCTimeout, req)
	if err != nil {
		return 0, err
	}

	return res.ProcessedTileCount, nil
}

func (d *dbNode) Fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	return d.tchanClient.TChannelClientFetch(defaultRPCTimeout, req)
}

func (d *dbNode) FetchTagged(req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	return d.tchanClient.TChannelClientFetchTagged(defaultRPCTimeout, req)
}

func (d *dbNode) Exec(commands ...string) (string, error) {
	//nolint:gosec
	cmd := exec.Command(commands[0], commands[1:]...)

	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}

	return out.String(), nil
}

func (d *dbNode) GoalStateExec(verifier resources.GoalStateVerifier, commands ...string) error {
	return retry(func() error {
		if err := verifier(d.Exec(commands...)); err != nil {
			d.logger.Info("goal state verification failed. retrying")
			return err
		}
		return nil
	})
}

func (d *dbNode) Restart() error {
	if err := d.Close(); err != nil {
		return err
	}

	d.start()

	return nil
}

func (d *dbNode) Close() error {
	defer func() {
		for _, dir := range d.tmpDirs {
			if err := os.RemoveAll(dir); err != nil {
				d.logger.Error("error removing temp directory", zap.String("dir", dir), zap.Error(err))
			}
		}
	}()

	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case d.interruptCh <- errors.New("in-process node being shut down"):
		case <-time.After(5 * time.Second):
			return errors.New("timeout sending interrupt. closing without graceful shutdown")
		}
	}

	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case <-d.shutdownCh:
		case <-time.After(1 * time.Minute):
			return errors.New("timeout waiting for shutdown notification. server closing may" +
				" not be completely graceful")
		}
	}

	return nil
}

func updateDBNodePorts(cfg config.Configuration) (config.Configuration, error) {
	if cfg.DB.ListenAddress != nil {
		addr, _, _, err := nettest.MaybeGeneratePort(*cfg.DB.ListenAddress)
		if err != nil {
			return cfg, err
		}

		cfg.DB.ListenAddress = &addr
	}

	if cfg.Coordinator != nil && cfg.Coordinator.ListenAddress != nil {
		addr, _, _, err := nettest.MaybeGeneratePort(*cfg.Coordinator.ListenAddress)
		if err != nil {
			return cfg, err
		}

		cfg.Coordinator.ListenAddress = &addr
	}

	return cfg, nil
}

func updateDBNodeFilepaths(cfg config.Configuration) (config.Configuration, []string, error) {
	tmpDirs := make([]string, 0, 1)

	prefix := cfg.DB.Filesystem.FilePathPrefix
	if prefix != nil && *prefix == "*" {
		dir, err := ioutil.TempDir("", "m3db-*")
		if err != nil {
			return cfg, tmpDirs, err
		}

		tmpDirs = append(tmpDirs, dir)
		cfg.DB.Filesystem.FilePathPrefix = &dir
	}

	return cfg, tmpDirs, nil
}

func retry(op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Second * 5
	bo.MaxElapsedTime = time.Minute
	return backoff.Retry(op, bo)
}
