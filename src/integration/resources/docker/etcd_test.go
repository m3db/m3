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

package docker

import (
	"context"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	testKey = "etcd-test"
)

var (
	testLogger, _ = zap.NewDevelopment()
)

type etcdTestDeps struct {
	Pool           *dockertest.Pool
	InstrumentOpts instrument.Options
	Etcd           *EtcdCluster
}

func setupEtcdTest(t *testing.T) etcdTestDeps {
	pool, err := dockertest.NewPool("")
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
				Endpoints: deps.Etcd.Members(),
			},
		)
		require.NoError(t, err)

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

		findContainers := func(namePrefix string, pool *dockertest.Pool) ([]docker.APIContainers, error) {
			containers, err := deps.Pool.Client.ListContainers(docker.ListContainersOptions{})
			if err != nil {
				return nil, err
			}

			var rtn []docker.APIContainers
			for _, ct := range containers {
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
	t.Run("errors when context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		deps := setupEtcdTest(t)
		deps.Etcd.newClient = func(config clientv3.Config) (memberClient, error) {
			return fakeMemberClient{err: assert.AnError}, nil
		}

		cancel()
		require.EqualError(
			t,
			deps.Etcd.waitForHealth(ctx),
			"waiting for etcd to become healthy: context cancelled while retrying: context canceled",
		)
	})
}

type fakeMemberClient struct {
	err error
}

func (f fakeMemberClient) MemberList(ctx context.Context) (*clientv3.MemberListResponse, error) {
	return nil, f.err
}

func newTestContext() (context.Context, func()) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}
