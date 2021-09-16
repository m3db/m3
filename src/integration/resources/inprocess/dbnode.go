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
	"net"
	"os/exec"
	"strconv"
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
	rpcPort int
	logger  *zap.Logger

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
	// Configure TChannel client for hitting the DB node.
	_, p, err := net.SplitHostPort(cfg.DB.ListenAddressOrDefault())
	if err != nil {
		return nil, err
	}

	cfg, err = updatePorts(cfg)
	if err != nil {
		return nil, err
	}

	rpcPort, err := strconv.Atoi(p)
	if err != nil {
		return nil, err
	}

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
		rpcPort:     rpcPort,
		logger:      opts.Logger,
		tchanClient: tchanClient,
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
	panic("implement me")
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
	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case d.interruptCh <- errors.New("in-process node being shut down"):
			break
		case <-time.After(5 * time.Second):
			return errors.New("timeout sending interrupt. closing without graceful shutdown")
		}
	}

	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case <-d.shutdownCh:
			break
		case <-time.After(1 * time.Minute):
			return errors.New("timeout waiting for shutdown notification. server closing may" +
				" not be completely graceful")
		}
	}

	return nil
}

func updatePorts(cfg config.Configuration) (config.Configuration, error) {
	if cfg.DB.ListenAddress != nil {
		h, p, err := net.SplitHostPort(*cfg.DB.ListenAddress)
		if err != nil {
			return cfg, err
		}

		if p == "0" {
			port, err := nettest.GetAvailablePort()
			if err != nil {
				return cfg, err
			}

			newAddr := net.JoinHostPort(h, strconv.Itoa(port))
			cfg.DB.ListenAddress = &newAddr
		}
	}

	return cfg, nil
}

func retry(op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Second * 5
	bo.MaxElapsedTime = time.Minute
	return backoff.Retry(op, bo)
}
