// Copyright (c) 2022 Uber Technologies, Inc.
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

package dockerm3

import (
	"fmt"
	"strings"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	xdockertest "github.com/m3db/m3/src/x/dockertest"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"go.uber.org/zap"
)

const (
	defaultDBNodeSource        = "dbnode"
	defaultDBNodeContainerName = "dbnode01"
)

var (
	defaultDBNodePortList = []int{2379, 2380, 9000, 9001, 9002, 9003, 9004}

	defaultDBNodeOptions = xdockertest.ResourceOptions{
		Source:        defaultDBNodeSource,
		ContainerName: defaultDBNodeContainerName,
		PortList:      defaultDBNodePortList,
	}
)

type dbNode struct {
	tchanClient *integration.TestTChannelClient
	resource    *xdockertest.Resource
	pool        *dockertest.Pool
	logger      *zap.Logger
}

func newDockerHTTPNode(
	pool *dockertest.Pool,
	opts xdockertest.ResourceOptions,
) (resources.Node, error) {
	opts = opts.WithDefaults(defaultDBNodeOptions)
	resource, err := xdockertest.NewDockerResource(pool, opts)
	if err != nil {
		return nil, err
	}

	completed := false
	defer func() {
		if !completed {
			_ = resource.Close()
		}
	}()

	logger := opts.InstrumentOpts.Logger()
	addr := resource.Resource().GetHostPort("9000/tcp")
	tchanClient, err := integration.NewTChannelClient("client", addr)
	if err != nil {
		return nil, err
	}

	logger.Info("set up tchanClient", zap.String("node_addr", addr))
	completed = true
	return &dbNode{
		tchanClient: tchanClient,
		pool:        pool,
		resource:    resource,
		logger:      logger,
	}, nil
}

func (c *dbNode) Start() {
	// noop as docker container should already be started
}

func (c *dbNode) HostDetails(p int) (*admin.Host, error) {
	var network docker.ContainerNetwork
	for _, n := range c.resource.Resource().Container.NetworkSettings.Networks { // nolint: gocritic
		network = n
	}

	host := strings.TrimLeft(c.resource.Resource().Container.Name, "/")
	return &admin.Host{
		Id:             host,
		IsolationGroup: "rack-a-" + c.resource.Resource().Container.Name,
		Zone:           "embedded",
		Weight:         1024,
		Address:        network.IPAddress,
		Port:           uint32(p),
	}, nil
}

func (c *dbNode) Health() (*rpc.NodeHealthResult_, error) {
	if c.resource.Closed() {
		return nil, xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("health"))
	res, err := c.tchanClient.TChannelClientHealth(timeout)
	if err != nil {
		logger.Error("failed get", zap.Error(err), zap.Any("res", res))
	}

	return res, err
}

func (c *dbNode) WaitForBootstrap() error {
	if c.resource.Closed() {
		return xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("waitForBootstrap"))
	return c.pool.Retry(func() error {
		health, err := c.Health()
		if err != nil {
			return err
		}

		if !health.GetBootstrapped() {
			err = fmt.Errorf("not bootstrapped")
			logger.Error("could not get health", zap.Error(err))
			return err
		}

		return nil
	})
}

func (c *dbNode) WritePoint(req *rpc.WriteRequest) error {
	if c.resource.Closed() {
		return xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("write"))
	err := c.tchanClient.TChannelClientWrite(timeout, req)
	if err != nil {
		logger.Error("could not write", zap.Error(err))
		return err
	}

	logger.Info("wrote")
	return nil
}

func (c *dbNode) WriteTaggedPoint(req *rpc.WriteTaggedRequest) error {
	if c.resource.Closed() {
		return xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("write-tagged"))
	err := c.tchanClient.TChannelClientWriteTagged(timeout, req)
	if err != nil {
		logger.Error("could not write-tagged", zap.Error(err))
		return err
	}

	logger.Info("wrote")
	return nil
}

// WriteTaggedBatchRaw writes a batch of writes to the node directly.
func (c *dbNode) WriteTaggedBatchRaw(req *rpc.WriteTaggedBatchRawRequest) error {
	if c.resource.Closed() {
		return xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("write-tagged-batch-raw"))
	err := c.tchanClient.TChannelClientWriteTaggedBatchRaw(timeout, req)
	if err != nil {
		logger.Error("writeTaggedBatchRaw call failed", zap.Error(err))
		return err
	}

	logger.Info("wrote")
	return nil
}

func (c *dbNode) AggregateTiles(req *rpc.AggregateTilesRequest) (int64, error) {
	if c.resource.Closed() {
		return 0, xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("aggregate-tiles"))
	rsp, err := c.tchanClient.TChannelClientAggregateTiles(timeout, req)
	if err != nil {
		logger.Error("could not aggregate tiles", zap.Error(err))
		return 0, err
	}

	logger.Info("wrote")
	return rsp.ProcessedTileCount, nil
}

func (c *dbNode) Fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	if c.resource.Closed() {
		return nil, xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("fetch"))
	dps, err := c.tchanClient.TChannelClientFetch(timeout, req)
	if err != nil {
		logger.Error("could not fetch", zap.Error(err))
		return nil, err
	}

	logger.Info("fetched", zap.Int("num_points", len(dps.GetDatapoints())))
	return dps, nil
}

func (c *dbNode) FetchTagged(req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	if c.resource.Closed() {
		return nil, xdockertest.ErrClosed
	}

	logger := c.logger.With(zapMethod("fetchtagged"))
	result, err := c.tchanClient.TChannelClientFetchTagged(timeout, req)
	if err != nil {
		logger.Error("could not fetch", zap.Error(err))
		return nil, err
	}

	logger.Info("fetched", zap.Int("series_count", len(result.GetElements())))
	return result, nil
}

func (c *dbNode) Restart() error {
	if c.resource.Closed() {
		return xdockertest.ErrClosed
	}

	cName := c.resource.Resource().Container.Name
	logger := c.logger.With(zapMethod("restart"))
	logger.Info("restarting container", zap.String("container", cName))
	err := c.pool.Client.RestartContainer(cName, 60)
	if err != nil {
		logger.Error("could not restart", zap.Error(err))
		return err
	}

	return nil
}

func (c *dbNode) Exec(commands ...string) (string, error) {
	if c.resource.Closed() {
		return "", xdockertest.ErrClosed
	}

	return c.resource.Exec(commands...)
}

func (c *dbNode) GoalStateExec(
	verifier resources.GoalStateVerifier,
	commands ...string,
) error {
	if c.resource.Closed() {
		return xdockertest.ErrClosed
	}

	return c.resource.GoalStateExec(verifier, commands...)
}

func (c *dbNode) Close() error {
	if c.resource.Closed() {
		return xdockertest.ErrClosed
	}

	return c.resource.Close()
}
