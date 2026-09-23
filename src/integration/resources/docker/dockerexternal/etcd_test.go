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
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	mobyclient "github.com/moby/moby/client"
	"github.com/ory/dockertest/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/x/instrument"
)

const (
	testKey = "etcd-test"
)

var (
	testLogger, _ = zap.NewDevelopment()
)

type etcdTestDeps struct {
	Pool           dockertest.Pool
	InstrumentOpts instrument.Options
	Etcd           *EtcdNode
}

func setupEtcdTest(t *testing.T) etcdTestDeps {
	pool, err := dockertest.NewPool(context.Background(), "")
	require.NoError(t, err)

	iopts := instrument.NewOptions().SetLogger(testLogger)
	c, err := NewEtcd(pool, iopts)
	require.NoError(t, err)

	return etcdTestDeps{
		Pool:           pool,
		Etcd:           c,
		InstrumentOpts: iopts,
	}
}

func TestClose_beforeSetup(t *testing.T) {
	// A node whose Setup never ran (or failed before the container came up)
	// has no resource to release. Close must be a no-op rather than a nil
	// pointer panic, since tests defer Close before checking Setup's error.
	ctx, cancel := newTestContext()
	defer cancel()

	node := &EtcdNode{logger: testLogger}
	require.NoError(t, node.Close(ctx))
}

func TestCluster(t *testing.T) {
	t.Run("starts a functioning cluster", func(t *testing.T) {
		ctx, cancel := newTestContext()
		defer cancel()

		deps := setupEtcdTest(t)
		require.NoError(t, deps.Etcd.Setup(ctx))

		t.Cleanup(func() {
			require.NoError(t, deps.Etcd.Close(ctx))
		})

		cli, err := clientv3.New(
			clientv3.Config{
				Endpoints: []string{deps.Etcd.Address()},
			},
		)
		require.NoError(t, err)

		//nolint:gosec
		testVal := strconv.Itoa(rand.Intn(10000))
		_, err = cli.Put(ctx, testKey, testVal)
		require.NoError(t, err)

		actualVal, err := cli.Get(ctx, testKey)
		require.NoError(t, err)

		assert.Equal(t, testVal, string(actualVal.Kvs[0].Value))
	})

	t.Run("can run multiple at once", func(t *testing.T) {
		ctx, cancel := newTestContext()
		defer cancel()

		deps := setupEtcdTest(t)

		require.NoError(t, deps.Etcd.Setup(ctx))
		defer func() {
			require.NoError(t, deps.Etcd.Close(ctx))
		}()

		c2, err := NewEtcd(deps.Pool, deps.InstrumentOpts)
		require.NoError(t, err)
		require.NoError(t, c2.Setup(ctx))
		defer func() {
			require.NoError(t, c2.Close(ctx))
		}()
	})

	t.Run("cleans up containers on shutdown", func(t *testing.T) {
		ctx, cancel := newTestContext()
		defer cancel()

		deps := setupEtcdTest(t)
		testPrefix := "cleanup-test-"
		deps.Etcd.namePrefix = testPrefix

		findContainers := func(namePrefix string, pool dockertest.Pool) ([]container.Summary, error) {
			containers, err := pool.Client().ContainerList(ctx, mobyclient.ContainerListOptions{})
			if err != nil {
				return nil, err
			}

			var rtn []container.Summary
			for _, ct := range containers.Items {
				for _, name := range ct.Names {
					// Docker response prefixes the container name with / regardless of what you give it as input.
					if strings.HasPrefix(name, "/"+namePrefix) {
						rtn = append(rtn, ct)
						break
					}
				}
			}
			return rtn, nil
		}

		require.NoError(t, deps.Etcd.Setup(ctx))

		cts, err := findContainers(testPrefix, deps.Pool)
		require.NoError(t, err)
		assert.Len(t, cts, 1)

		require.NoError(t, deps.Etcd.Close(ctx))
		cts, err = findContainers(testPrefix, deps.Pool)
		require.NoError(t, err)
		assert.Len(t, cts, 0)
	})
}

func TestCluster_waitForHealth(t *testing.T) {
	t.Run("errors when context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		deps := setupEtcdTest(t)
		etcdCli := fakeMemberClient{err: assert.AnError}
		cancel()
		require.EqualError(
			t,
			deps.Etcd.waitForHealth(ctx, etcdCli),
			"waiting for etcd to become healthy: context canceled while retrying: context canceled",
		)
	})
}

func TestHealthCheckContext(t *testing.T) {
	t.Run("grants the floor when the parent deadline is already blown", func(t *testing.T) {
		// Mimics a container pull that overran the caller's whole budget.
		parent, cancelParent := context.WithTimeout(context.Background(), -2*time.Minute)
		defer cancelParent()
		require.Error(t, parent.Err(), "parent should already be expired")

		ctx, cancel := healthCheckContext(parent)
		defer cancel()

		require.NoError(t, ctx.Err())
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.Greater(t, time.Until(deadline), minHealthCheckTimeout/2)
	})

	t.Run("keeps a parent deadline that leaves enough room", func(t *testing.T) {
		want := 10 * minHealthCheckTimeout
		parent, cancelParent := context.WithTimeout(context.Background(), want)
		defer cancelParent()

		ctx, cancel := healthCheckContext(parent)
		defer cancel()

		parentDeadline, ok := parent.Deadline()
		require.True(t, ok)
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.Equal(t, parentDeadline, deadline)
	})

	t.Run("propagates cancellation when the parent deadline is kept", func(t *testing.T) {
		parent, cancelParent := context.WithTimeout(context.Background(), 10*minHealthCheckTimeout)
		defer cancelParent()

		ctx, cancel := healthCheckContext(parent)
		defer cancel()

		cancelParent()
		assert.ErrorIs(t, ctx.Err(), context.Canceled)
	})

	t.Run("bounds a parentless context", func(t *testing.T) {
		ctx, cancel := healthCheckContext(context.Background())
		defer cancel()

		_, ok := ctx.Deadline()
		assert.False(t, ok, "should not invent a deadline the caller did not set")
	})
}

type fakeMemberClient struct {
	err error
}

func (f fakeMemberClient) MemberList(
	ctx context.Context,
	_ ...clientv3.OpOption,
) (*clientv3.MemberListResponse, error) {
	return nil, f.err
}

func newTestContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}
