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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	m3err "github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var (
	errRead     = errors.New("read error")
	initialPort = 17762
	testMu      sync.Mutex
)

func generateAddress() string {
	testMu.Lock()
	defer testMu.Unlock()
	address := fmt.Sprintf("localhost:%d", initialPort)
	initialPort++
	return address
}

type mockStorage struct {
	t           *testing.T
	read        *storage.FetchQuery
	sleepMillis int
}

func (s *mockStorage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.FetchResult, error) {
	return nil, m3err.ErrNotImplemented
}

func (s *mockStorage) FetchTags(
	ctx context.Context,
	query *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, nil
}

func (s *mockStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, m3err.ErrNotImplemented
}

func (s *mockStorage) FetchRaw(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (encoding.SeriesIterators, error) {
	it, _ := test.BuildTestSeriesIterator()
	its := encoding.NewSeriesIterators(
		[]encoding.SeriesIterator{it},
		nil,
	)

	if s.sleepMillis > 0 {
		time.Sleep(time.Millisecond * time.Duration(s.sleepMillis))
	}

	return its, nil
}

func (s *mockStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return nil
}

func (s *mockStorage) Type() storage.Type {
	return storage.Type(0)
}

func (s *mockStorage) Close() error {
	return nil
}

func checkRemoteFetch(t *testing.T, its encoding.SeriesIterators) {
	iters := its.Iters()
	require.Len(t, iters, 1)
	validateSeries(t, iters[0])
}

func startServer(t *testing.T, host string, store storage.Storage) {
	ip := &mockIteratorPool{}
	server := CreateNewGrpcServer(store, ip)
	waitForStart := make(chan struct{})
	go func() {
		err := StartNewGrpcServer(server, host, waitForStart)
		assert.NoError(t, err)
	}()
	<-waitForStart
}

func createStorageFetchOptions() *storage.FetchOptions {
	return &storage.FetchOptions{
		KillChan: make(chan struct{}),
	}
}

func createCtxReadOpts(t *testing.T) (context.Context, *storage.FetchQuery, *storage.FetchOptions, string) {
	logging.InitWithCores(nil)

	ctx := context.Background()
	read, _, _ := createStorageFetchQuery(t)
	readOpts := createStorageFetchOptions()
	host := generateAddress()
	return ctx, read, readOpts, host
}

func checkFetch(ctx context.Context, t *testing.T, client Client, read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	fetch, err := client.FetchRaw(ctx, read, readOpts)
	require.NoError(t, err)
	checkRemoteFetch(t, fetch)
}

func checkErrorFetch(ctx context.Context, t *testing.T, client Client, read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	fetch, err := client.FetchRaw(ctx, read, readOpts)
	assert.Nil(t, fetch)
	assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))
}

func buildClient(t *testing.T, hosts []string) Client {
	ip := &mockIteratorPool{}
	client, err := NewGrpcClient(hosts, ip, nil, grpc.WithBlock())
	require.NoError(t, err)
	return client
}

func TestRpc(t *testing.T) {
	ctx, read, readOpts, host := createCtxReadOpts(t)
	store := &mockStorage{
		t:    t,
		read: read,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client := buildClient(t, hosts)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	checkFetch(ctx, t, client, read, readOpts)
}

func TestRpcMultipleRead(t *testing.T) {
	ctx, read, readOpts, host := createCtxReadOpts(t)
	store := &mockStorage{
		t:    t,
		read: read,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client := buildClient(t, hosts)
	defer func() {
		assert.NoError(t, client.Close())
	}()
	fetch, err := client.FetchRaw(ctx, read, readOpts)
	require.NoError(t, err)
	checkRemoteFetch(t, fetch)
}

func TestRpcStopsStreamingWhenFetchKilledOnClient(t *testing.T) {
	ctx, read, readOpts, host := createCtxReadOpts(t)
	sleepMillis := 100
	store := &mockStorage{
		t:           t,
		read:        read,
		sleepMillis: sleepMillis,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client := buildClient(t, hosts)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	go func() {
		time.Sleep(time.Millisecond * 10)
		readOpts.KillChan <- struct{}{}
	}()
	fetch, err := client.FetchRaw(ctx, read, readOpts)
	require.Nil(t, fetch)
	assert.Equal(t, err, m3err.ErrQueryInterrupted)
}

func TestMultipleClientRpc(t *testing.T) {
	ctx, read, readOpts, host := createCtxReadOpts(t)
	store := &mockStorage{
		t:           t,
		read:        read,
		sleepMillis: 10,
	}
	startServer(t, host, store)

	var wg sync.WaitGroup
	clients := make([]Client, 10)
	for i := range clients {
		hosts := []string{host}
		clients[i] = buildClient(t, hosts)
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

type errStorage struct {
	t    *testing.T
	read *storage.FetchQuery
}

func (s *errStorage) Fetch(
	ctx context.Context,
	query *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.FetchResult, error) {
	readQueriesAreEqual(s.t, s.read, query)
	return nil, m3err.ErrNotImplemented
}

func (s *errStorage) FetchRaw(
	ctx context.Context,
	query *storage.FetchQuery,
	_ *storage.FetchOptions,
) (encoding.SeriesIterators, error) {
	return nil, errRead
}

func (s *errStorage) FetchBlocks(
	ctx context.Context,
	query *storage.FetchQuery,
	_ *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, m3err.ErrNotImplemented
}

func (s *errStorage) FetchTags(
	ctx context.Context,
	query *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, m3err.ErrNotImplemented
}

func (s *errStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	return m3err.ErrNotImplemented
}

func (s *errStorage) Type() storage.Type {
	return storage.Type(-1)
}

func (s *errStorage) Close() error {
	return nil
}

func TestEmptyAddressListErrors(t *testing.T) {
	addresses := []string{}
	client, err := NewGrpcClient(addresses, nil, nil)
	assert.Nil(t, client)
	assert.Equal(t, m3err.ErrNoClientAddresses, err)
}

func TestErrRpc(t *testing.T) {
	ctx, read, readOpts, host := createCtxReadOpts(t)
	store := &errStorage{
		t:    t,
		read: read,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client := buildClient(t, hosts)
	defer func() {
		assert.NoError(t, client.Close())
	}()

	checkErrorFetch(ctx, t, client, read, readOpts)
}

func TestRoundRobinClientRpc(t *testing.T) {
	ctx, read, readOpts, host := createCtxReadOpts(t)
	store := &mockStorage{
		t:    t,
		read: read,
	}
	startServer(t, host, store)
	errHost := generateAddress()
	errStore := &errStorage{
		t:    t,
		read: read,
	}
	startServer(t, errHost, errStore)

	hosts := []string{host, errHost}
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
		fetch, err := client.FetchRaw(ctx, read, readOpts)
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
