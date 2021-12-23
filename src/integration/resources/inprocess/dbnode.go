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
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/google/uuid"
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
	"github.com/m3db/m3/src/x/config/hostid"
	xos "github.com/m3db/m3/src/x/os"
)

// TODO(nate): make configurable
const defaultRPCTimeout = time.Minute

// DBNode is an in-process implementation of resources.Node.
type DBNode struct {
	cfg     config.Configuration
	logger  *zap.Logger
	tmpDirs []string
	started bool
	startFn DBNodeStartFn

	interruptCh chan<- error
	shutdownCh  <-chan struct{}
	// tchanClient is an RPC client used for hitting the DB nodes RPC API.
	tchanClient *integration.TestTChannelClient
}

//nolint:maligned
// DBNodeOptions are options for starting a DB node server.
type DBNodeOptions struct {
	// GeneratePorts will automatically update the config to use open ports
	// if set to true. If false, configuration is used as-is re: ports.
	GeneratePorts bool
	// GenerateHostID will automatically update the host ID specified in
	// the config if set to true. If false, configuration is used as-is re: host ID.
	GenerateHostID bool
	// StartFn is a custom function that can be used to start the DBNode.
	StartFn DBNodeStartFn
	// Start indicates whether to start the dbnode instance.
	Start bool
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
// and options provided. Use NewDBNode or any of the convenience constructors
// (e.g. NewDBNodeFromYAML, NewDBNodeFromConfigFile) to get a running
// dbnode.
//
// The most typical usage of this method will be in an integration test to validate
// some behavior. For example, assuming we have a valid placement available already we
// could do the following to read and write to a namespace (note: ignoring error checking):
//
//    dbnode, _ := NewDBNodeFromYAML(defaultDBNodeConfig, DBNodeOptions{})
//    dbnode.WaitForBootstrap()
//    dbnode.WriteTaggedPoint(&rpc.WriteTaggedRequest{...}))
//    res, _ = dbnode.FetchTagged(&rpc.FetchTaggedRequest{...})
//
// The dbnode will start up as you specify in your config. However, there is some
// helper logic to avoid port and filesystem collisions when spinning up multiple components
// within the process. If you specify a GeneratePorts: true in the DBNodeOptions, address ports
// will be replaced with an open port.
//
// Similarly, filepath fields will  be updated with a temp directory that will be cleaned up
// when the dbnode is destroyed. This should ensure that many of the same component can be
// spun up in-process without any issues with collisions.
func NewDBNode(cfg config.Configuration, opts DBNodeOptions) (resources.Node, error) {
	// Massage config so it runs properly in tests.
	cfg, tmpDirs, err := updateDBNodeConfig(cfg, opts)
	if err != nil {
		return nil, err
	}

	hostID, err := cfg.DB.HostIDOrDefault().Resolve()
	if err != nil {
		return nil, err
	}
	logging := cfg.DB.LoggingOrDefault()
	if len(logging.Fields) == 0 {
		logging.Fields = make(map[string]interface{})
	}
	logging.Fields["component"] = fmt.Sprintf("dbnode:%s", hostID)
	cfg.DB.Logging = &logging

	// Configure TChannel client for hitting the DB node.
	tchanClient, err := integration.NewTChannelClient("client", cfg.DB.ListenAddressOrDefault())
	if err != nil {
		return nil, err
	}

	// Configure logger
	if opts.Logger == nil {
		opts.Logger, err = resources.NewLogger()
		if err != nil {
			return nil, err
		}
	}

	// Start the DB node
	node := &DBNode{
		cfg:         cfg,
		logger:      opts.Logger,
		tchanClient: tchanClient,
		tmpDirs:     tmpDirs,
		startFn:     opts.StartFn,
	}
	if opts.Start {
		node.Start()
	}

	return node, nil
}

// Start starts the DBNode instance
func (d *DBNode) Start() {
	if d.started {
		d.logger.Debug("dbnode already started")
		return
	}
	d.started = true

	if d.startFn != nil {
		d.interruptCh, d.shutdownCh = d.startFn(&d.cfg)
		return
	}

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

// HostDetails returns this node's host details on the given port.
func (d *DBNode) HostDetails(_ int) (*admin.Host, error) {
	_, p, err := net.SplitHostPort(d.cfg.DB.ListenAddressOrDefault())
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(p)
	if err != nil {
		return nil, err
	}

	hostID, err := d.cfg.DB.HostIDOrDefault().Resolve()
	if err != nil {
		return nil, err
	}

	discoverCfg := d.cfg.DB.DiscoveryOrDefault()
	envConfig, err := discoverCfg.EnvironmentConfig(hostID)
	if err != nil {
		return nil, err
	}

	return &admin.Host{
		Id: hostID,
		// TODO(nate): add support for multiple etcd services. Practically, this
		// is very rare so using the zero-indexed value here will almost always be
		// correct.
		Zone: envConfig.Services[0].Service.Zone,
		// TODO(nate): weight should most likely not live here as it's part of
		// cluster configuration
		Weight:  1024,
		Address: "0.0.0.0",
		Port:    uint32(port),
	}, nil
}

// Health gives this node's health.
func (d *DBNode) Health() (*rpc.NodeHealthResult_, error) {
	return d.tchanClient.TChannelClientHealth(defaultRPCTimeout)
}

// WaitForBootstrap blocks until the node has bootstrapped.
func (d *DBNode) WaitForBootstrap() error {
	return resources.Retry(func() error {
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

// WritePoint writes a datapoint to the node directly.
func (d *DBNode) WritePoint(req *rpc.WriteRequest) error {
	return d.tchanClient.TChannelClientWrite(defaultRPCTimeout, req)
}

// WriteTaggedPoint writes a datapoint with tags to the node directly.
func (d *DBNode) WriteTaggedPoint(req *rpc.WriteTaggedRequest) error {
	return d.tchanClient.TChannelClientWriteTagged(defaultRPCTimeout, req)
}

// WriteTaggedBatchRaw writes a batch of writes to the node directly.
func (d *DBNode) WriteTaggedBatchRaw(req *rpc.WriteTaggedBatchRawRequest) error {
	return d.tchanClient.TChannelClientWriteTaggedBatchRaw(defaultRPCTimeout, req)
}

// AggregateTiles starts tiles aggregation, waits until it will complete
// and returns the amount of aggregated tiles.
func (d *DBNode) AggregateTiles(req *rpc.AggregateTilesRequest) (int64, error) {
	res, err := d.tchanClient.TChannelClientAggregateTiles(defaultRPCTimeout, req)
	if err != nil {
		return 0, err
	}

	return res.ProcessedTileCount, nil
}

// Fetch fetches datapoints.
func (d *DBNode) Fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	return d.tchanClient.TChannelClientFetch(defaultRPCTimeout, req)
}

// FetchTagged fetches datapoints by tag.
func (d *DBNode) FetchTagged(req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	return d.tchanClient.TChannelClientFetchTagged(defaultRPCTimeout, req)
}

// Exec executes the given commands on the node container, returning
// stdout and stderr from the container.
func (d *DBNode) Exec(commands ...string) (string, error) {
	//nolint:gosec
	cmd := exec.Command(commands[0], commands[1:]...)

	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}

	return out.String(), nil
}

// GoalStateExec executes the given commands on the node container, retrying
// until applying the verifier returns no error or the default timeout.
func (d *DBNode) GoalStateExec(verifier resources.GoalStateVerifier, commands ...string) error {
	return resources.Retry(func() error {
		if err := verifier(d.Exec(commands...)); err != nil {
			d.logger.Info("goal state verification failed. retrying")
			return err
		}
		return nil
	})
}

// Restart restarts this container.
func (d *DBNode) Restart() error {
	if err := d.Close(); err != nil {
		return err
	}

	d.Start()

	return nil
}

// Close closes the wrapper and releases any held resources, including
// deleting docker containers.
func (d *DBNode) Close() error {
	defer func() {
		for _, dir := range d.tmpDirs {
			if err := os.RemoveAll(dir); err != nil {
				d.logger.Error("error removing temp directory", zap.String("dir", dir), zap.Error(err))
			}
		}
	}()

	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case d.interruptCh <- xos.NewInterruptError("in-process node being shut down"):
		case <-time.After(interruptTimeout):
			return errors.New("timeout sending interrupt. closing without graceful shutdown")
		}
	}

	for i := 0; i < d.cfg.Components(); i++ {
		select {
		case <-d.shutdownCh:
		case <-time.After(shutdownTimeout):
			return errors.New("timeout waiting for shutdown notification. server closing may" +
				" not be completely graceful")
		}
	}
	d.started = false

	return nil
}

// Configuration returns a copy of the configuration used to
// start this dbnode.
func (d *DBNode) Configuration() config.Configuration {
	return d.cfg
}

func updateDBNodeConfig(
	cfg config.Configuration,
	opts DBNodeOptions,
) (config.Configuration, []string, error) {
	var (
		tmpDirs []string
		err     error
	)
	// Replace any ports with open ports
	if opts.GeneratePorts {
		cfg, err = updateDBNodePorts(cfg)
		if err != nil {
			return config.Configuration{}, nil, err
		}
	}

	// Replace host ID configuration with config-based version.
	if opts.GenerateHostID {
		cfg = updateDBNodeHostID(cfg)
	}

	// Replace any filepath with a temporary directory
	cfg, tmpDirs, err = updateDBNodeFilepaths(cfg)
	if err != nil {
		return config.Configuration{}, nil, err
	}

	return cfg, tmpDirs, nil
}

func updateDBNodePorts(cfg config.Configuration) (config.Configuration, error) {
	addr1, _, err := nettest.GeneratePort(cfg.DB.ListenAddressOrDefault())
	if err != nil {
		return cfg, err
	}
	cfg.DB.ListenAddress = &addr1

	addr2, _, err := nettest.GeneratePort(cfg.DB.ClusterListenAddressOrDefault())
	if err != nil {
		return cfg, err
	}
	cfg.DB.ClusterListenAddress = &addr2

	addr3, _, err := nettest.GeneratePort(cfg.DB.HTTPNodeListenAddressOrDefault())
	if err != nil {
		return cfg, err
	}
	cfg.DB.HTTPNodeListenAddress = &addr3

	addr4, _, err := nettest.GeneratePort(cfg.DB.HTTPClusterListenAddressOrDefault())
	if err != nil {
		return cfg, err
	}
	cfg.DB.HTTPClusterListenAddress = &addr4

	addr5, _, err := nettest.GeneratePort(cfg.DB.DebugListenAddressOrDefault())
	if err != nil {
		return cfg, err
	}
	cfg.DB.DebugListenAddress = &addr5

	if cfg.Coordinator != nil {
		coordCfg, err := updateCoordinatorPorts(*cfg.Coordinator)
		if err != nil {
			return cfg, err
		}

		cfg.Coordinator = &coordCfg
	}

	return cfg, nil
}

func updateDBNodeHostID(cfg config.Configuration) config.Configuration {
	hostID := uuid.New().String()
	cfg.DB.HostID = &hostid.Configuration{
		Resolver: hostid.ConfigResolver,
		Value:    &hostID,
	}

	return cfg
}

func updateDBNodeFilepaths(cfg config.Configuration) (config.Configuration, []string, error) {
	tmpDirs := make([]string, 0, 1)

	dir, err := ioutil.TempDir("", "m3db-*")
	if err != nil {
		return cfg, nil, err
	}
	tmpDirs = append(tmpDirs, dir)
	cfg.DB.Filesystem.FilePathPrefix = &dir

	ec := cfg.DB.Client.EnvironmentConfig
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

	if cfg.Coordinator != nil {
		coordCfg, coordDirs, err := updateCoordinatorFilepaths(*cfg.Coordinator)
		if err != nil {
			return cfg, nil, err
		}
		tmpDirs = append(tmpDirs, coordDirs...)

		cfg.Coordinator = &coordCfg
	}

	return cfg, tmpDirs, nil
}
