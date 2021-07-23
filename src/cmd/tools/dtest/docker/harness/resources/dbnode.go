// Copyright (c) 2020 Uber Technologies, Inc.
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

package resources

import (
	"fmt"
	"strings"
	"sync"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	xerrors "github.com/m3db/m3/src/x/errors"

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

	defaultDBNodeOptions = dockerResourceOptions{
		source:        defaultDBNodeSource,
		containerName: defaultDBNodeContainerName,
		portList:      defaultDBNodePortList,
	}
)

// GoalStateVerifier verifies that the given results are valid.
type GoalStateVerifier func(string, error) error

// Nodes is a slice of nodes.
type Nodes []Node

func (n Nodes) waitForHealthy() error {
	var (
		multiErr xerrors.MultiError
		mu       sync.Mutex
		wg       sync.WaitGroup
	)

	for _, node := range n {
		wg.Add(1)
		node := node
		go func() {
			defer wg.Done()
			err := node.WaitForBootstrap()
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return multiErr.FinalError()
}

// Node is a wrapper for a db node. It provides a wrapper on HTTP
// endpoints that expose cluster management APIs as well as read and write
// endpoints for series data.
// TODO: consider having this work on underlying structures.
type Node interface {
	// HostDetails returns this node's host details on the given port.
	HostDetails(port int) (*admin.Host, error)
	// Health gives this node's health.
	Health() (*rpc.NodeHealthResult_, error)
	// WaitForBootstrap blocks until the node has bootstrapped.
	WaitForBootstrap() error
	// WritePoint writes a datapoint to the node directly.
	WritePoint(req *rpc.WriteRequest) error
	// WriteTaggedPoint writes a datapoint with tags to the node directly.
	WriteTaggedPoint(req *rpc.WriteTaggedRequest) error
	// AggregateTiles starts tiles aggregation, waits until it will complete
	// and returns the amount of aggregated tiles.
	AggregateTiles(req *rpc.AggregateTilesRequest) (int64, error)
	// Fetch fetches datapoints.
	Fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error)
	// FetchTagged fetches datapoints by tag.
	FetchTagged(req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error)
	// Exec executes the given commands on the node container, returning
	// stdout and stderr from the container.
	Exec(commands ...string) (string, error)
	// GoalStateExec executes the given commands on the node container, retrying
	// until applying the verifier returns no error or the default timeout.
	GoalStateExec(verifier GoalStateVerifier, commands ...string) error
	// Restart restarts this container.
	Restart() error
	// Close closes the wrapper and releases any held resources, including
	// deleting docker containers.
	Close() error
}

type dbNode struct {
	tchanClient *integration.TestTChannelClient
	resource    *dockerResource
}

func newDockerHTTPNode(
	pool *dockertest.Pool,
	opts dockerResourceOptions,
) (Node, error) {
	opts = opts.withDefaults(defaultDBNodeOptions)
	resource, err := newDockerResource(pool, opts)
	if err != nil {
		return nil, err
	}

	completed := false
	defer func() {
		if !completed {
			resource.close()
		}
	}()

	addr := resource.resource.GetHostPort("9000/tcp")
	tchanClient, err := integration.NewTChannelClient("client", addr)
	if err != nil {
		return nil, err
	}

	resource.logger.Info("set up tchanClient", zap.String("node_addr", addr))
	completed = true
	return &dbNode{
		tchanClient: tchanClient,
		resource:    resource,
	}, nil
}

func (c *dbNode) HostDetails(p int) (*admin.Host, error) {
	var network docker.ContainerNetwork
	for _, n := range c.resource.resource.Container.NetworkSettings.Networks { // nolint: gocritic
		network = n
	}

	host := strings.TrimLeft(c.resource.resource.Container.Name, "/")
	return &admin.Host{
		Id:             host,
		IsolationGroup: "rack-a-" + c.resource.resource.Container.Name,
		Zone:           "embedded",
		Weight:         1024,
		Address:        network.IPAddress,
		Port:           uint32(p),
	}, nil
}

func (c *dbNode) Health() (*rpc.NodeHealthResult_, error) {
	if c.resource.closed {
		return nil, errClosed
	}

	logger := c.resource.logger.With(zapMethod("health"))
	res, err := c.tchanClient.TChannelClientHealth(timeout)
	if err != nil {
		logger.Error("failed get", zap.Error(err), zap.Any("res", res))
	}

	return res, err
}

func (c *dbNode) WaitForBootstrap() error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("waitForBootstrap"))
	return c.resource.pool.Retry(func() error {
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
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("write"))
	err := c.tchanClient.TChannelClientWrite(timeout, req)
	if err != nil {
		logger.Error("could not write", zap.Error(err))
		return err
	}

	logger.Info("wrote")
	return nil
}

func (c *dbNode) WriteTaggedPoint(req *rpc.WriteTaggedRequest) error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("write-tagged"))
	err := c.tchanClient.TChannelClientWriteTagged(timeout, req)
	if err != nil {
		logger.Error("could not write-tagged", zap.Error(err))
		return err
	}

	logger.Info("wrote")
	return nil
}

func (c *dbNode) AggregateTiles(req *rpc.AggregateTilesRequest) (int64, error) {
	if c.resource.closed {
		return 0, errClosed
	}

	logger := c.resource.logger.With(zapMethod("aggregate-tiles"))
	rsp, err := c.tchanClient.TChannelClientAggregateTiles(timeout, req)
	if err != nil {
		logger.Error("could not aggregate tiles", zap.Error(err))
		return 0, err
	}

	logger.Info("wrote")
	return rsp.ProcessedTileCount, nil
}

func (c *dbNode) Fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	if c.resource.closed {
		return nil, errClosed
	}

	logger := c.resource.logger.With(zapMethod("fetch"))
	dps, err := c.tchanClient.TChannelClientFetch(timeout, req)
	if err != nil {
		logger.Error("could not fetch", zap.Error(err))
		return nil, err
	}

	logger.Info("fetched", zap.Int("num_points", len(dps.GetDatapoints())))
	return dps, nil
}

func (c *dbNode) FetchTagged(req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error) {
	if c.resource.closed {
		return nil, errClosed
	}

	logger := c.resource.logger.With(zapMethod("fetchtagged"))
	result, err := c.tchanClient.TChannelClientFetchTagged(timeout, req)
	if err != nil {
		logger.Error("could not fetch", zap.Error(err))
		return nil, err
	}

	logger.Info("fetched", zap.Int("series_count", len(result.GetElements())))
	return result, nil
}

func (c *dbNode) Restart() error {
	if c.resource.closed {
		return errClosed
	}

	cName := c.resource.resource.Container.Name
	logger := c.resource.logger.With(zapMethod("restart"))
	logger.Info("restarting container", zap.String("container", cName))
	err := c.resource.pool.Client.RestartContainer(cName, 60)
	if err != nil {
		logger.Error("could not restart", zap.Error(err))
		return err
	}

	return nil
}

func (c *dbNode) Exec(commands ...string) (string, error) {
	if c.resource.closed {
		return "", errClosed
	}

	return c.resource.exec(commands...)
}

func (c *dbNode) GoalStateExec(
	verifier GoalStateVerifier,
	commands ...string,
) error {
	if c.resource.closed {
		return errClosed
	}

	return c.resource.goalStateExec(verifier, commands...)
}

func (c *dbNode) Close() error {
	if c.resource.closed {
		return errClosed
	}

	return c.resource.close()
}
