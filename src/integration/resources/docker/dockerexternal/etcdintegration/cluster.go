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

// Package etcdintegration is a mostly drop-in replacement for the etcd integration
// (github.com/etcd-io/etcd/tests/v3/framework/integration) package.
// Instead of starting etcd within this Go process, it starts etcd using a docker container.
package etcdintegration

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/m3db/m3/src/integration/resources/docker/dockerexternal"
	"github.com/m3db/m3/src/integration/resources/docker/dockerexternal/etcdintegration/bridge"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	xtest "github.com/m3db/m3/src/x/test"
)

const (
	startTimeout = 30 * time.Second
	stopTimeout  = 30 * time.Second

	clientHealthTimeout = 30 * time.Second
)

// ClusterConfig configures an etcd integration test cluster.
type ClusterConfig struct {
	// Size is the number of nodes in the cluster. Provided as a parameter to be API compatible with the etcd package,
	// but currently only one node is supported.
	Size int

	// UseBridge enables a networking bridge on etcd members, accessible via Node.Bridge(). This allows manipulation
	// of connections to particular members.
	UseBridge bool
}

// Cluster is an etcd cluster. Currently, the implementation is such that only one node clusters are allowed.
type Cluster struct {
	// Members are the etcd nodes that make up the cluster.
	Members []*Node

	terminated bool
}

// NewCluster starts an etcd cluster using docker.
func NewCluster(t testingT, cfg *ClusterConfig) *Cluster {
	if cfg.Size > 1 {
		t.Errorf("NewCluster currently only supports single node clusters")
		t.FailNow()
		return nil
	}

	// N.B.: this used to use zaptest, but that introduces a hard dependency on t.Log. t.Log can't be run after
	// a test finishes, which means that any goroutines that still access this logger after a test has finished will
	// cause it to panic. This led to flakes for some time, which we mostly were able to ignore because of auto
	// retries in buildkite.
	// Ideally, all tests cleanup all goroutines, but in practice for integration tests, that's a bit of a tall order.
	// TestMultiServerForwardingPipelineDiscardNaNAggregatedValues fails fairly  consistently with zaptest here.
	logger := xtest.NewLogger(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	r, err := dockerexternal.NewEtcd(pool, instrument.NewOptions(), dockerexternal.EtcdClusterUseBridge(cfg.UseBridge))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()

	cluster := &Cluster{
		Members: []*Node{newNode(r, logger, cfg)},
	}

	require.NoError(t, cluster.start(ctx))

	// Paranoia: try to ensure that we cleanup the containers, even if our callers mess up.
	t.Cleanup(func() {
		if !cluster.terminated {
			cluster.Terminate(t)
		}
	})
	return cluster
}

// start is private because NewCluster is intended to always start the cluster.
func (c *Cluster) start(ctx context.Context) error {
	var merr xerrors.MultiError
	for _, m := range c.Members {
		merr = merr.Add(m.start(ctx))
	}
	if err := merr.FinalError(); err != nil {
		return fmt.Errorf("failed starting etcd cluster: %w", err)
	}
	return nil
}

// RandClient returns a client from any member in the cluster.
func (c *Cluster) RandClient() *clientv3.Client {
	//nolint:gosec
	return c.Members[rand.Intn(len(c.Members))].Client
}

// Terminate stops all nodes in the cluster.
func (c *Cluster) Terminate(t testingT) {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()

	c.terminated = true

	var err xerrors.MultiError
	for _, node := range c.Members {
		err = err.Add(node.close(ctx))
	}
	require.NoError(t, err.FinalError())
}

// Node is a single etcd server process, running in a docker container.
type Node struct {
	Client *clientv3.Client

	resource dockerEtcd
	cfg      *ClusterConfig
	logger   *zap.Logger
	bridge   *bridge.Bridge
}

func newNode(r dockerEtcd, logger *zap.Logger, cfg *ClusterConfig) *Node {
	return &Node{
		resource: r,
		logger:   logger,
		cfg:      cfg,
	}
}

// Stop stops the etcd container, but doesn't remove it.
func (n *Node) Stop(t testingT) {
	ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	require.NoError(t, n.resource.Stop(ctx))

	if n.bridge != nil {
		n.bridge.Close()
	}
}

// Bridge can be used to manipulate connections to this etcd node. It
// is a man-in-the-middle listener which mostly transparently forwards connections, unless told to drop them via e.g.
// the Blackhole method.
// Bridge will only be active if cfg.UseBridge is true; calling this method otherwise will panic.
func (n *Node) Bridge() *bridge.Bridge {
	if !n.cfg.UseBridge {
		panic("EtcdNode wasn't configured to use a Bridge; pass EtcdClusterUseBridge(true) to enable.")
	}
	return n.bridge
}

// Restart starts a stopped etcd container, stopping it first if it's not already.
func (n *Node) Restart(t testingT) error {
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	require.NoError(t, n.resource.Restart(ctx))
	return nil
}

// start starts the etcd node. It is private because it isn't part of the etcd/integration package API, and
// should only be called by Cluster.start.
func (n *Node) start(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	if err := n.resource.Setup(ctx); err != nil {
		return fmt.Errorf("starting etcd container: %w", err)
	}

	address := n.resource.Address()
	if n.cfg.UseBridge {
		addr, err := n.setupBridge()
		if err != nil {
			return fmt.Errorf("setting up connection bridge for etcd node: %w", err)
		}
		address = addr
	}

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + address},
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
		DialTimeout: 5 * time.Second,
		Logger:      n.logger,
	})

	if err != nil {
		return fmt.Errorf("constructing etcd client for member: %w", err)
	}

	n.logger.Info("Connecting to docker etcd using host machine port",
		zap.String("endpoint", address),
	)

	n.Client = etcdCli
	return nil
}

// setupBridge puts a man-in-the-middle listener in between the etcd docker process and the client. See Bridge() for
// details.
// Returns the new address of the bridge, which clients should connect to.
func (n *Node) setupBridge() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("setting up listener for bridge: %w", err)
	}

	n.logger.Info("etcd bridge is listening", zap.String("addr", listener.Addr().String()))

	// dialer = make connections to the etcd container
	// listener = the bridge's inbounds
	n.bridge, err = bridge.New(dialer{hostport: n.resource.Address()}, listener)
	if err != nil {
		return "", err
	}

	return listener.Addr().String(), nil
}

func (n *Node) close(ctx context.Context) error {
	var err xerrors.MultiError
	err = err.Add(n.Client.Close())
	return err.Add(n.resource.Close(ctx)).FinalError()
}

type dialer struct {
	hostport string
}

func (d dialer) Dial() (net.Conn, error) {
	return net.Dial("tcp", d.hostport)
}

// testingT wraps *testing.T. Allows us to not directly depend on *testing package.
type testingT interface {
	require.TestingT

	Cleanup(func())
}

// BeforeTestExternal -- solely here to match etcd API's.
func BeforeTestExternal(t testingT) {}

// WaitClientV3 waits for an etcd client to be healthy.
func WaitClientV3(t testingT, kv clientv3.KV) {
	ctx, cancel := context.WithTimeout(context.Background(), clientHealthTimeout)
	defer cancel()

	err := retry.NewRetrier(retry.NewOptions().SetForever(true)).AttemptContext(
		ctx,
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_, err := kv.Get(ctx, "/")
			return err
		},
	)

	require.NoError(t, err)
}
