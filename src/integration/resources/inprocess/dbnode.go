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

type dbNode struct {
	cfg     config.Configuration
	logger  *zap.Logger
	tmpDirs []string
	started bool

	interruptCh chan<- error
	shutdownCh  <-chan struct{}
	// tchanClient is an RPC client used for hitting the DB nodes RPC API.
	tchanClient *integration.TestTChannelClient
}

// DBNodeOptions are options for starting a DB node server.
type DBNodeOptions struct {
	// GeneratePorts will automatically update the config to use open ports
	// if set to true. If false, configuration is used as-is re: ports.
	GeneratePorts bool
	// GenerateHostID will automatically update the host ID specified in
	// the config if set to true. If false, configuration is used as-is re: host ID.
	GenerateHostID bool
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
		opts.Logger, err = newLogger()
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
	d.started = true
}

func (d *dbNode) HostDetails(_ int) (*admin.Host, error) {
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
		// TODO(nate): weight should be configurable
		Weight:  1024,
		Address: "0.0.0.0",
		Port:    uint32(port),
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
