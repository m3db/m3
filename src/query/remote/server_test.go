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
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	m3err "github.com/m3db/m3/src/query/errors"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var (
	testName     = "remote_foo"
	errRead      = errors.New("read error")
	poolsWrapper = pools.NewPoolsWrapper(
		pools.BuildIteratorPools(pools.BuildIteratorPoolsOptions{}))
)

type mockStorageOptions struct {
	err                  error
	iters                encoding.SeriesIterators
	fetchCompressedSleep time.Duration
	cleanup              func() error
}

func newMockStorage(
	t *testing.T,
	ctrl *gomock.Controller,
	opts mockStorageOptions,
) *m3.MockStorage {
	store := m3.NewMockStorage(ctrl)

	store.EXPECT().FetchCompressedResult(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context,
		query *storage.FetchQuery,
		options *storage.FetchOptions,
	) (consolidators.SeriesFetchResult, m3.Cleanup, error) {
		var cleanup = func() error { return nil }
		if opts.cleanup != nil {
			cleanup = opts.cleanup
		}

		if opts.err != nil {
			return consolidators.SeriesFetchResult{
				Metadata: block.NewResultMetadata(),
			}, cleanup, opts.err
		}

		if opts.fetchCompressedSleep > 0 {
			time.Sleep(opts.fetchCompressedSleep)
		}

		iters := opts.iters
		if iters == nil {
			it, err := test.BuildTestSeriesIterator(seriesID)
			require.NoError(t, err)
			iters = encoding.NewSeriesIterators(
				[]encoding.SeriesIterator{it},
				nil,
			)
		}

		res, err := consolidators.NewSeriesFetchResult(
			iters,
			nil,
			block.NewResultMetadata(),
		)
		return res, cleanup, err
	}).AnyTimes()
	return store
}

func checkRemoteFetch(t *testing.T, r storage.PromResult) {
	res := r.PromResult
	seriesList := res.GetTimeseries()
	require.Equal(t, 1, len(seriesList))

	for _, series := range seriesList {
		datapoints := series.GetSamples()
		values := make([]float64, 0, len(datapoints))
		for _, d := range datapoints {
			values = append(values, d.GetValue())
		}

		require.Equal(t, expectedValues(), values)
	}
}

func startServer(t *testing.T, ctrl *gomock.Controller,
	store m3.Storage) net.Listener {
	server := NewGRPCServer(store, models.QueryContextOptions{},
		poolsWrapper, instrument.NewOptions())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	return listener
}

func createCtxReadOpts(t *testing.T) (context.Context,
	*storage.FetchQuery, *storage.FetchOptions) {
	ctx := context.Background()
	read, _, _ := createStorageFetchQuery(t)
	readOpts := storage.NewFetchOptions()
	readOpts.SeriesLimit = 300
	return ctx, read, readOpts
}

func checkFetch(ctx context.Context, t *testing.T, client Client,
	read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	fetch, err := client.FetchProm(ctx, read, readOpts)
	require.NoError(t, err)
	checkRemoteFetch(t, fetch)
}

func checkErrorFetch(ctx context.Context, t *testing.T, client Client,
	read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	_, err := client.FetchProm(ctx, read, readOpts)
	assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))
}

func buildClient(t *testing.T, hosts []string) Client {
	readWorkerPool, err := xsync.NewPooledWorkerPool(runtime.NumCPU(),
		xsync.NewPooledWorkerPoolOptions())
	readWorkerPool.Init()
	require.NoError(t, err)

	opts := m3db.NewOptions().
		SetReadWorkerPool(readWorkerPool).
		SetTagOptions(models.NewTagOptions())

	client, err := NewGRPCClient(testName, hosts, poolsWrapper, opts,
		instrument.NewTestOptions(t), grpc.WithBlock())
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

func TestRpcHealth(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, _, _ := createCtxReadOpts(t)
	store := newMockStorage(t, ctrl, mockStorageOptions{})
	listener := startServer(t, ctrl, store)
	serverClient := buildClient(t, []string{listener.Addr().String()})
	defer func() {
		assert.NoError(t, serverClient.Close())
	}()

	client, ok := serverClient.(*grpcClient)
	require.True(t, ok)

	resp, err := client.client.Health(ctx, &rpc.HealthRequest{})
	require.NoError(t, err)

	uptime, err := time.ParseDuration(resp.UptimeDuration)
	require.NoError(t, err)
	assert.True(t, uptime > 0)
	assert.Equal(t, uptime, time.Duration(resp.UptimeNanoseconds))
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

	fetch, err := client.FetchProm(ctx, read, readOpts)
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

	_, err := client.FetchProm(ctx, read, readOpts)

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
	addresses := []string{}
	opts := m3db.NewOptions()
	client, err := NewGRPCClient(testName, addresses, poolsWrapper, opts,
		instrument.NewTestOptions(t), grpc.WithBlock())
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

	// Host ordering is not always deterministic; retry several times to ensure
	// at least one call is made to both hosts. Giving 10 attempts per host should
	// remove flakiness while guaranteeing round robin behaviour.
	attempts := 20

	hitHost, hitErrHost := false, false
	for i := 0; i < attempts; i++ {
		fetch, err := client.FetchProm(ctx, read, readOpts)
		if err != nil {
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

func TestBatchedFetch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, read, readOpts := createCtxReadOpts(t)
	exNames := []string{"baz", "foo"}
	exValues := []string{"qux", "bar"}
	sizes := []int{0, 1, defaultBatch - 1, defaultBatch,
		defaultBatch + 1, defaultBatch*2 + 1}

	for _, size := range sizes {
		var (
			msg     = fmt.Sprintf("batch size: %d", size)
			iters   = make([]encoding.SeriesIterator, 0, size)
			cleaned = false
		)

		for i := 0; i < size; i++ {
			id := fmt.Sprintf("%s_%d", seriesID, i)
			it, err := test.BuildTestSeriesIterator(id)
			require.NoError(t, err, msg)
			iters = append(iters, it)
		}

		store := newMockStorage(t, ctrl, mockStorageOptions{
			iters: encoding.NewSeriesIterators(iters, nil),
			cleanup: func() error {
				require.False(t, cleaned, msg)
				cleaned = true
				return nil
			},
		})

		listener := startServer(t, ctrl, store)
		client := buildClient(t, []string{listener.Addr().String()})
		defer func() {
			assert.NoError(t, client.Close())
		}()

		fetch, err := client.FetchProm(ctx, read, readOpts)
		require.NoError(t, err, msg)
		seriesList := fetch.PromResult.GetTimeseries()
		require.Equal(t, size, len(seriesList), msg)
		for _, series := range seriesList {
			samples := series.GetSamples()
			values := make([]float64, 0, len(samples))
			for _, d := range samples {
				values = append(values, d.GetValue())
			}

			require.Equal(t, expectedValues(), values, msg)
			require.Equal(t, 2, len(series.GetLabels()))
			for i, l := range series.GetLabels() {
				assert.Equal(t, exNames[i], string(l.Name))
				assert.Equal(t, exValues[i], string(l.Value))
			}
		}

		require.True(t, cleaned, msg)
	}
}

func TestBatchedSearch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, q, readOpts := createCtxReadOpts(t)
	sizes := []int{0, 1, defaultBatch - 1, defaultBatch,
		defaultBatch + 1, defaultBatch*2 + 1}
	for _, size := range sizes {
		var (
			msg     = fmt.Sprintf("batch size: %d", size)
			tags    = make([]consolidators.MultiTagResult, 0, size)
			names   = make([]string, 0, size)
			cleaned = false
		)

		noopCleanup := func() error {
			require.False(t, cleaned, msg)
			cleaned = true
			return nil
		}

		for i := 0; i < size; i++ {
			name := fmt.Sprintf("%s_%d", seriesID, i)
			tag := consolidators.MultiTagResult{
				ID: ident.StringID(name),
				Iter: ident.NewTagsIterator(ident.NewTags(
					ident.Tag{
						Name:  ident.StringID(name),
						Value: ident.StringID(name),
					},
				)),
			}

			tags = append(tags, tag)
			names = append(names, name)
		}

		store := m3.NewMockStorage(ctrl)
		tagResult := consolidators.TagResult{
			Tags:     tags,
			Metadata: block.NewResultMetadata(),
		}

		store.EXPECT().SearchCompressed(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(tagResult, noopCleanup, nil)

		listener := startServer(t, ctrl, store)
		client := buildClient(t, []string{listener.Addr().String()})
		defer func() {
			assert.NoError(t, client.Close())
		}()

		result, err := client.SearchSeries(ctx, q, readOpts)
		require.NoError(t, err, msg)
		require.Equal(t, size, len(result.Metrics), msg)

		for i, m := range result.Metrics {
			n := names[i]
			require.Equal(t, n, string(m.ID), msg)
		}

		require.True(t, cleaned, msg)
	}
}

func TestBatchedCompleteTags(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx, _, readOpts := createCtxReadOpts(t)
	namesOnly := []bool{true, false}
	for _, nameOnly := range namesOnly {
		q := &storage.CompleteTagsQuery{
			CompleteNameOnly: nameOnly,
		}

		sizes := []int{0, 1, defaultBatch - 1, defaultBatch,
			defaultBatch + 1, defaultBatch*2 + 1}
		for _, size := range sizes {
			var (
				msg  = fmt.Sprintf("batch size: %d, name only: %t", size, nameOnly)
				tags = make([]consolidators.CompletedTag, 0, size)
			)

			for i := 0; i < size; i++ {
				name := fmt.Sprintf("%s_%d", seriesID, i)
				tag := consolidators.CompletedTag{
					Name: []byte(name),
				}

				if !nameOnly {
					tag.Values = [][]byte{[]byte("a"), []byte("b")}
				}

				tags = append(tags, tag)
			}

			store := m3.NewMockStorage(ctrl)
			expected := &consolidators.CompleteTagsResult{
				CompleteNameOnly: nameOnly,
				CompletedTags:    tags,
				Metadata: block.ResultMetadata{
					Exhaustive: false,
					LocalOnly:  true,
					Warnings:   []block.Warning{block.Warning{Name: "foo", Message: "bar"}},
				},
			}

			store.EXPECT().CompleteTagsCompressed(gomock.Any(), gomock.Any(), gomock.Any()).
				Return(expected, nil)

			listener := startServer(t, ctrl, store)
			client := buildClient(t, []string{listener.Addr().String()})
			defer func() {
				assert.NoError(t, client.Close())
			}()

			result, err := client.CompleteTags(ctx, q, readOpts)
			require.NoError(t, err, msg)
			require.Equal(t, size, len(result.CompletedTags), msg)
			if size == 0 {
				// NB: 0 result is exhaustive and no warnings should be seen.
				expected.Metadata = block.NewResultMetadata()
			} else {
				// NB: since this is a fanout with remotes, LocalOnly should be false.
				expected.Metadata.LocalOnly = false
			}
			assert.Equal(t, expected, result, msg)
		}
	}
}
