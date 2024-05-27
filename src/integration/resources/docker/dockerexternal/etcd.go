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

package dockerexternal

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/m3db/m3/src/integration/resources/docker/dockerexternal/etcdintegration/bridge"
	xdockertest "github.com/m3db/m3/src/x/dockertest"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

var (
	etcdImage = xdockertest.Image{
		Name: "quay.io/coreos/etcd",
		Tag:  "v3.5.7",
	}
)

// NewEtcd constructs a single etcd node, running in a docker container.
func NewEtcd(
	pool *dockertest.Pool,
	instrumentOpts instrument.Options,
	options ...EtcdClusterOption,
) (*EtcdNode, error) {
	logger := instrumentOpts.Logger()
	if logger == nil {
		logger = zap.NewNop()
		instrumentOpts = instrumentOpts.SetLogger(logger)
	}

	var opts etcdClusterOptions
	for _, o := range options {
		o.apply(&opts)
	}

	return &EtcdNode{
		pool:           pool,
		instrumentOpts: instrumentOpts,
		logger:         logger,
		opts:           opts,
		// Solely for mocking in tests--unfortunately we don't want to take in the etcd client as a dependency here
		// (we don't know the endpoints, and therefore need to construct it ourselves).
		// Thus, we do two hops (mock newClient returning mock memberClient)
		newClient: func(config clientv3.Config) (memberClient, error) {
			return clientv3.New(config)
		},
	}, nil
}

// EtcdNode is a single etcd node, running via a docker container.
//
//nolint:maligned
type EtcdNode struct {
	instrumentOpts instrument.Options
	logger         *zap.Logger
	pool           *dockertest.Pool
	opts           etcdClusterOptions

	// namePrefix is used to name the cluster. Exists solely for unittests in this package; otherwise a const
	namePrefix string
	newClient  func(config clientv3.Config) (memberClient, error)

	// initialized by Setup
	address  string
	resource *xdockertest.Resource
	etcdCli  *clientv3.Client
	bridge   *bridge.Bridge

	stopped bool
}

// Setup starts the docker container.
func (c *EtcdNode) Setup(ctx context.Context) (closeErr error) {
	if c.resource != nil {
		return errors.New("etcd cluster already started")
	}

	// nolint:gosec
	id := rand.New(rand.NewSource(time.Now().UnixNano())).Int()

	namePrefix := "m3-test-etcd-"
	if c.namePrefix != "" {
		// support overriding for tests
		namePrefix = c.namePrefix
	}

	// Roughly, runs:

	// Instructions from: https: //etcd.io/docs/v3.3/op-guide/container/
	//
	// docker run --rm \
	//  --name Etcd \
	//  quay.io/coreos/etcd:v3.5.7 \
	//	--env ALLOW_NONE_AUTHENTICATION=yes \
	// 	--name node1 \
	//	--listen-peer-urls http://0.0.0.0:2380 \
	//	--initial-advertise-peer-urls http://127.0.0.1:2380 \
	//	--listen-client-urls http://0.0.0.0:2379 \
	//	--advertise-client-urls http://127.0.0.1:2379 \
	//	--initial-cluster node1=http://127.0.0.1:2380"
	//
	// Port 2379 on the container is bound to a free port on the host
	resource, err := xdockertest.NewDockerResource(c.pool, xdockertest.ResourceOptions{
		OverrideDefaults: false,
		// TODO: what even is this?
		Source: "etcd",

		ContainerName:  fmt.Sprintf("%s%d", namePrefix, id),
		Image:          etcdImage,
		Env:            []string{"ALLOW_NONE_AUTHENTICATION=yes"},
		InstrumentOpts: c.instrumentOpts,
		PortMappings: map[docker.Port][]docker.PortBinding{
			"2379/tcp": {{
				HostIP:   "0.0.0.0",
				HostPort: strconv.Itoa(c.opts.port),
			}},
		},
		Cmd: []string{
			"/usr/local/bin/etcd",
			"--name", "node1",
			"--listen-peer-urls", "http://0.0.0.0:2380",
			"--initial-advertise-peer-urls", "http://127.0.0.1:2380",

			"--listen-client-urls", "http://0.0.0.0:2379",
			"--advertise-client-urls", "http://127.0.0.1:2379",
			"--initial-cluster", "node1=http://127.0.0.1:2380",
		},
		NoNetworkOverlay: true,
	})

	if err != nil {
		return fmt.Errorf("starting etcd container: %w", err)
	}

	defer func() {
		// If we errored somewhere in here, make sure to close the etcd client (callers won't necessarily do it).
		if closeErr != nil {
			closeErr = xerrors.NewMultiError().
				Add(closeErr).
				Add(resource.Close()).
				FinalError()
		}
	}()

	container := resource.Resource().Container
	c.logger.Info("etcd container started",
		zap.String("containerID", container.ID),
		zap.Any("ports", container.NetworkSettings.Ports),
		// Uncomment if you need gory details about the container printed; equivalent of `docker inspect <id>
		zap.Any("container", container),
	)
	// Extract the port on which we are listening.
	// This is coming from the equivalent of docker inspect <container_id>
	portBinds := container.NetworkSettings.Ports["2379/tcp"]

	c.resource = resource
	c.address = fmt.Sprintf("%s:%s", "127.0.0.1", portBinds[0].HostPort)

	etcdCli, err := clientv3.New(
		clientv3.Config{
			Endpoints:   []string{c.address},
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{grpc.WithBlock()},
			Logger:      c.logger,
		},
	)
	if err != nil {
		return fmt.Errorf("constructing etcd client to test for cluster health: %w", err)
	}

	defer func() {
		if err := etcdCli.Close(); err != nil {
			var merr xerrors.MultiError
			closeErr = merr.
				Add(closeErr).
				Add(fmt.Errorf("closing etcd client: %w", err)).
				FinalError()
		}
	}()

	return c.waitForHealth(ctx, etcdCli)
}

func (c *EtcdNode) containerHostPort() string {
	portBinds := c.resource.Resource().Container.NetworkSettings.Ports["2379/tcp"]

	return fmt.Sprintf("127.0.0.1:%s", portBinds[0].HostPort)
}

func (c *EtcdNode) waitForHealth(ctx context.Context, memberCli memberClient) error {
	retrier := retry.NewRetrier(retry.NewOptions().
		SetForever(true).
		SetMaxBackoff(5 * time.Second),
	)

	var timeout time.Duration
	deadline, ok := ctx.Deadline()
	if ok {
		timeout = deadline.Sub(time.Now())
	}
	c.logger.Info(
		"Waiting for etcd to report healthy (via member list)",
		zap.String("timeout", timeout.String()),
	)
	err := retrier.AttemptContext(ctx, func() error {
		_, err := memberCli.MemberList(ctx)
		if err != nil {
			c.logger.Info(
				"Failed connecting to etcd while waiting for container to come up",
				zap.Error(err),
				zap.String("endpoints", c.address),
			)
		}
		return err
	})
	if err == nil {
		c.logger.Info("etcd is healthy")
		return nil
	}
	return fmt.Errorf("waiting for etcd to become healthy: %w", err)
}

// Close stops the etcd node, and removes it.
func (c *EtcdNode) Close(ctx context.Context) error {
	var err xerrors.MultiError
	err = err.
		Add(c.resource.Close())
	return err.FinalError()
}

// Address is the host:port of the etcd node for use by etcd clients.
func (c *EtcdNode) Address() string {
	return c.address
}

// Stop stops the etcd container, but does not purge it. A stopped container can be restarted with Restart.
func (c *EtcdNode) Stop(ctx context.Context) error {
	if c.stopped {
		return errors.New("etcd node is already stopped")
	}
	if err := c.pool.Client.StopContainerWithContext(c.resource.Resource().Container.ID, 0, ctx); err != nil {
		return err
	}
	c.stopped = true
	return nil
}

// Restart restarts the etcd container. If it isn't currently stopped, the etcd container will be stopped and then
// started; else it will just be start.
func (c *EtcdNode) Restart(ctx context.Context) error {
	if !c.stopped {
		c.logger.Info("Stopping etcd node")

		if err := c.Stop(ctx); err != nil {
			return fmt.Errorf("stopping etcd node for Restart: %w", err)
		}
	}
	err := c.pool.Client.StartContainerWithContext(c.resource.Resource().Container.ID, nil, ctx)
	if err != nil {
		return fmt.Errorf("starting etcd node for Restart: %w", err)
	}
	c.stopped = false
	return nil
}

var _ memberClient = (*clientv3.Client)(nil)

// memberClient exposes just one method of *clientv3.Client, for purposes of tests.
type memberClient interface {
	MemberList(ctx context.Context) (*clientv3.MemberListResponse, error)
}
