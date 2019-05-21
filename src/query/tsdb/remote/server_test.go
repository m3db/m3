// Copyright (c) 2018 Uber Technologies, Inc.
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

package remote

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	m3err "github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/util/logging"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var (
	errRead      = errors.New("read error")
	poolsWrapper = pools.NewPoolsWrapper(pools.BuildIteratorPools())
)

type mockStorageOptions struct {
	err                  error
	iters                encoding.SeriesIterators
	fetchCompressedSleep time.Duration
}

func newMockStorage(
	t *testing.T,
	ctrl *gomock.Controller,
	opts mockStorageOptions,
) *m3.MockStorage {
	store := m3.NewMockStorage(ctrl)
	store.EXPECT().
		FetchCompressed(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			query *storage.FetchQuery,
			options *storage.FetchOptions,
		) (encoding.SeriesIterators, m3.Cleanup, error) {
			noopCleanup := func() error { return nil }

			if opts.err != nil {
				return nil, noopCleanup, opts.err
			}

			if opts.fetchCompressedSleep > 0 {
				time.Sleep(opts.fetchCompressedSleep)
			}

			iters := opts.iters
			if iters == nil {
				it, err := test.BuildTestSeriesIterator()
				require.NoError(t, err)
				iters = encoding.NewSeriesIterators(
					[]encoding.SeriesIterator{it},
					nil,
				)
			}

			return iters, noopCleanup, nil
		}).
		AnyTimes()
	return store
}

func checkRemoteFetch(t *testing.T, r *storage.FetchResult) {
	require.Equal(t, 1, len(r.SeriesList))

	for _, series := range r.SeriesList {
		datapoints := series.Values().Datapoints()
		values := make([]float64, 0, len(datapoints))
		for _, d := range datapoints {
			values = append(values, d.Value)
		}

		require.Equal(t, expectedValues(), values)
	}
}

func startServer(t *testing.T, ctrl *gomock.Controller, store m3.Storage) net.Listener {
	server := NewGRPCServer(store, models.QueryContextOptions{}, poolsWrapper)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	return listener
}

func createCtxReadOpts(t *testing.T) (context.Context, *storage.FetchQuery, *storage.FetchOptions) {
	logging.InitWithCores(nil)

	ctx := context.Background()
	read, _, _ := createStorageFetchQuery(t)
	readOpts := storage.NewFetchOptions()
	return ctx, read, readOpts
}

func checkFetch(ctx context.Context, t *testing.T, client Client, read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	fetch, err := client.Fetch(ctx, read, readOpts)
	require.NoError(t, err)
	checkRemoteFetch(t, fetch)
}

func checkErrorFetch(ctx context.Context, t *testing.T, client Client, read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	fetch, err := client.Fetch(ctx, read, readOpts)
	assert.Nil(t, fetch)
	assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))
}

func buildClient(t *testing.T, hosts []string) Client {
	readWorkerPool, err := xsync.NewPooledWorkerPool(runtime.NumCPU(),
		xsync.NewPooledWorkerPoolOptions())
	readWorkerPool.Init()
	require.NoError(t, err)
	client, err := NewGRPCClient(hosts, poolsWrapper, readWorkerPool,
		models.NewTagOptions(), 0, grpc.WithBlock())
	require.NoError(t, err)
	return client
}

func TestRpc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, read, readOpts := createCtxReadOpts(t)
	store := newMockStorage(t, ctrl, mockStorageOptions{})
	listener := startServer(t, ctrl, store)
	client := buildClient(t, []string{listener.Addr().String()})
	defer func() {
		assert.NoError(t, client.Close())
	}()

	checkFetch(ctx, t, client, read, readOpts)
}

func TestRpcMultipleRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, read, readOpts := createCtxReadOpts(t)
	store := newMockStorage(t, ctrl, mockStorageOptions{})

	listener := startServer(t, ctrl, store)
	client := buildClient(t, []string{listener.Addr().String()})
	defer func() {
		assert.NoError(t, client.Close())
	}()

	fetch, err := client.Fetch(ctx, read, readOpts)
	require.NoError(t, err)
	checkRemoteFetch(t, fetch)
}

func TestRpcStopsStreamingWhenFetchKilledOnClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, read, readOpts := createCtxReadOpts(t)
	store := newMockStorage(t, ctrl, mockStorageOptions{
		fetchCompressedSleep: time.Second,
	})

	listener := startServer(t, ctrl, store)
	client := buildClient(t, []string{listener.Addr().String()})
	defer func() {
		assert.NoError(t, client.Close())
	}()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	fetch, err := client.Fetch(ctx, read, readOpts)
	require.Nil(t, fetch)
	require.Error(t, err)
}

func TestMultipleClientRpc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, read, readOpts := createCtxReadOpts(t)
	store := newMockStorage(t, ctrl, mockStorageOptions{
		fetchCompressedSleep: 10 * time.Millisecond,
	})

	listener := startServer(t, ctrl, store)

	var wg sync.WaitGroup
	clients := make([]Client, 10)
	for i := range clients {
		clients[i] = buildClient(t, []string{listener.Addr().String()})
	}

	defer func() {
		for _, client := range clients {
			assert.NoError(t, client.Close())
		}
	}()

	for _, client := range clients {
		wg.Add(1)
		client := client
		go func() {
			checkFetch(ctx, t, client, read, readOpts)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestEmptyAddressListErrors(t *testing.T) {
	readWorkerPool, err := xsync.NewPooledWorkerPool(runtime.NumCPU(),
		xsync.NewPooledWorkerPoolOptions())
	require.NoError(t, err)
	readWorkerPool.Init()

	addresses := []string{}
	client, err := NewGRPCClient(addresses, poolsWrapper, readWorkerPool,
		models.NewTagOptions(), 0, grpc.WithBlock())
	assert.Nil(t, client)
	assert.Equal(t, m3err.ErrNoClientAddresses, err)
}

func TestErrRpc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, read, readOpts := createCtxReadOpts(t)
	store := newMockStorage(t, ctrl, mockStorageOptions{
		err: errors.New("read error"),
	})

	listener := startServer(t, ctrl, store)
	client := buildClient(t, []string{listener.Addr().String()})
	defer func() {
		assert.NoError(t, client.Close())
	}()

	checkErrorFetch(ctx, t, client, read, readOpts)
}

func TestRoundRobinClientRpc(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, read, readOpts := createCtxReadOpts(t)
	store := newMockStorage(t, ctrl, mockStorageOptions{})
	errStore := newMockStorage(t, ctrl, mockStorageOptions{
		err: errors.New("read error"),
	})

	listener1 := startServer(t, ctrl, store)
	listener2 := startServer(t, ctrl, errStore)

	hosts := []string{listener1.Addr().String(), listener2.Addr().String()}
	client := buildClient(t, hosts)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	// Host ordering is not always deterministic; retry several times to ensure at least one
	// call is made to both hosts. Giving 10 attempts per host should remove flakiness while guaranteeing
	// round robin behaviour
	attempts := 20

	hitHost, hitErrHost := false, false
	for i := 0; i < attempts; i++ {
		fetch, err := client.Fetch(ctx, read, readOpts)
		if fetch == nil {
			assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))
			hitErrHost = true
		} else {
			checkRemoteFetch(t, fetch)
			hitHost = true
		}
		if hitHost && hitErrHost {
			break
		}
	}

	assert.True(t, hitHost, "round robin did not fetch from host")
	assert.True(t, hitErrHost, "round robin did not fetch from error host")
}

func validateBlockResult(t *testing.T, r block.Result) {
	require.Equal(t, 1, len(r.Blocks))

	_, err := r.Blocks[0].SeriesIter()
	require.NoError(t, err)
}
