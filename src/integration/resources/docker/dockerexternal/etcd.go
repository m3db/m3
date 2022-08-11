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
	"time"

	xdockertest "github.com/m3db/m3/src/x/dockertest"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
	"github.com/ory/dockertest/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func NewEtcd(pool *dockertest.Pool, instrumentOpts instrument.Options) (*EtcdCluster, error) {

	logger := instrumentOpts.Logger()
	if logger == nil {
		logger = zap.NewNop()
		instrumentOpts = instrumentOpts.SetLogger(logger)
	}
	return &EtcdCluster{
		pool:           pool,
		instrumentOpts: instrumentOpts,
		logger:         logger,

		// Solely for mocking in tests--unfortunately we don't want to take in the etcd client as a dependency here
		// (we don't know the endpoints, and therefore need to construct it ourselves).
		// Thus, we do two hops (mock newClient returning mock memberClient)
		newClient: func(config clientv3.Config) (memberClient, error) {
			return clientv3.New(config)
		},
	}, nil
}

type EtcdCluster struct {
	instrumentOpts instrument.Options
	logger         *zap.Logger
	pool           *dockertest.Pool

	// namePrefix is used to name the cluster. Exists solely for unittests in this package; otherwise a const
	namePrefix string
	newClient  func(config clientv3.Config) (memberClient, error)

	// initialized by Setup
	members  []string
	resource *xdockertest.Resource
}

func (c *EtcdCluster) Setup(ctx context.Context) error {
	if c.resource != nil {
		return errors.New("etcd cluster already started")
	}

	if err := xdockertest.SetupNetwork(c.pool, false); err != nil {
		return err
	}
	id := rand.Int()

	namePrefix := "m3-test-etcd-"
	if c.namePrefix != "" {
		// support overriding for tests
		namePrefix = c.namePrefix
	}

	// Roughly, runs:
	// docker run --rm --env ALLOW_NONE_AUTHENTICATION=yes -it --name Etcd bitnami/etcd
	// Port 2379 on the container is bound to a free port on the host
	resource, err := xdockertest.NewDockerResource(c.pool, xdockertest.ResourceOptions{
		OverrideDefaults: false,
		// TODO: what even is this?
		Source:        "etcd",
		ContainerName: fmt.Sprintf("%s%d", namePrefix, id),
		Image: xdockertest.Image{
			Name: "bitnami/etcd",
		},
		Env:            []string{"ALLOW_NONE_AUTHENTICATION=yes"},
		InstrumentOpts: c.instrumentOpts,
		//Mounts:           nil,
		//TmpfsMounts:      nil,
		//InstrumentOpts:   nil,
	})

	if err != nil {
		return fmt.Errorf("starting etcd container: %w", err)
	}

	// Extract the port on which we are listening.
	// This is coming from the equivalent of docker inspect <container_id>
	portBinds := resource.Resource().Container.NetworkSettings.Ports["2379/tcp"]

	c.members = []string{fmt.Sprintf("127.0.0.1:%s", portBinds[0].HostPort)}
	c.resource = resource
	return c.waitForHealth(ctx)
}

func (c *EtcdCluster) waitForHealth(ctx context.Context) error {
	retrier := retry.NewRetrier(retry.NewOptions().
		SetForever(true).
		SetMaxBackoff(5 * time.Second),
	)

	etcdCli, err := c.newClient(
		clientv3.Config{
			Endpoints: c.members,
		},
	)
	if err != nil {
		return fmt.Errorf("constructing etcd client to check health: %w", err)
	}

	c.logger.Info("Waiting for etcd to report healthy (via member list)")
	err = retrier.AttemptContext(ctx, func() error {
		_, err := etcdCli.MemberList(ctx)
		if err != nil {
			c.logger.Info(
				"Failed connecting to etcd while waiting for container to come up",
				zap.Error(err),
				zap.Strings("endpoints", c.members),
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

func (c *EtcdCluster) Close(ctx context.Context) error {
	return c.resource.Close()
}

func (c *EtcdCluster) Members() []string {
	return c.members
}

var _ memberClient = (*clientv3.Client)(nil)

// memberClient exposes just one method of *clientv3.Client
type memberClient interface {
	MemberList(ctx context.Context) (*clientv3.MemberListResponse, error)
}
