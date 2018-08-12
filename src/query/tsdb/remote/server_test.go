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

	"github.com/m3db/m3/src/query/block"
	m3err "github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	name = "aa"
	mps  = 5
)

var (
	tags        = models.Tags{"1": "b", "2": "c"}
	values      = []float64{1.0, 2.0, 3.0, 4.0}
	errWrite    = errors.New("write error")
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

func makeValues() ts.Values {
	vals := ts.NewFixedStepValues(mps, len(values), 0, time.Now())
	for i, v := range values {
		vals.SetValueAt(i, v)
	}
	return vals
}

func makeSeries() *ts.Series {
	return ts.NewSeries(name, makeValues(), tags)
}

type mockStorage struct {
	t           *testing.T
	read        *storage.FetchQuery
	write       *storage.WriteQuery
	sleepMillis int
	numPages    int
	mu          sync.Mutex
}

func (s *mockStorage) Fetch(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.FetchResult, error) {
	readQueriesAreEqual(s.t, s.read, query)

	if s.sleepMillis > 0 {
		time.Sleep(time.Millisecond * time.Duration(s.sleepMillis))
	}

	s.mu.Lock()
	s.numPages--
	hasNext := s.numPages > 0
	s.mu.Unlock()

	tsSeries := []*ts.Series{makeSeries()}
	return &storage.FetchResult{
		SeriesList: tsSeries,
		LocalOnly:  false,
		HasNext:    hasNext,
	}, nil
}

func (s *mockStorage) FetchTags(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.SearchResults, error) {
	return nil, nil
}

func (s *mockStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	writeQueriesAreEqual(s.t, s.write, query)
	return nil
}

func (s *mockStorage) Type() storage.Type {
	return storage.Type(0)
}

func (s *mockStorage) FetchBlocks(
	ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (block.Result, error) {
	return block.Result{}, fmt.Errorf("not implemented")
}

func (s *mockStorage) Close() error {
	return nil
}

func checkMultipleRemoteFetch(t *testing.T, res *storage.FetchResult, numResults int) {
	assert.False(t, res.LocalOnly)
	require.Len(t, res.SeriesList, numResults)
	for _, s := range res.SeriesList {
		assert.Equal(t, name, s.Name())
		assert.Equal(t, tags, s.Tags)
		assert.Equal(t, len(values), s.Len())
	}
}

func checkRemoteFetch(t *testing.T, res *storage.FetchResult) {
	checkMultipleRemoteFetch(t, res, 1)
}

func startServer(t *testing.T, host string, store storage.Storage) {
	server := CreateNewGrpcServer(store)
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

func createCtxReadWriteOpts(t *testing.T) (context.Context, *storage.FetchQuery, *storage.WriteQuery, *storage.FetchOptions, string) {
	logging.InitWithCores(nil)

	ctx := context.Background()
	read, _, _ := createStorageFetchQuery(t)
	write, _ := createStorageWriteQuery(t)
	readOpts := createStorageFetchOptions()
	host := generateAddress()
	return ctx, read, write, readOpts, host
}

func checkFetch(ctx context.Context, t *testing.T, client Client, read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	fetch, err := client.Fetch(ctx, read, readOpts)
	require.NoError(t, err)
	checkRemoteFetch(t, fetch)
}

func checkWrite(ctx context.Context, t *testing.T, client Client, write *storage.WriteQuery) {
	err := client.Write(ctx, write)
	require.Nil(t, err)
}

func checkErrorFetch(ctx context.Context, t *testing.T, client Client, read *storage.FetchQuery, readOpts *storage.FetchOptions) {
	fetch, err := client.Fetch(ctx, read, readOpts)
	assert.Nil(t, fetch)
	assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))
}

func checkErrorWrite(ctx context.Context, t *testing.T, client Client, write *storage.WriteQuery) {
	err := client.Write(ctx, write)
	assert.Equal(t, errWrite.Error(), grpc.ErrorDesc(err))
}

func TestRpc(t *testing.T) {
	ctx, read, write, readOpts, host := createCtxReadWriteOpts(t)
	store := &mockStorage{
		t:     t,
		read:  read,
		write: write,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client, err := NewGrpcClient(hosts, grpc.WithBlock())
	require.NoError(t, err)
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()

	checkWrite(ctx, t, client, write)
	checkFetch(ctx, t, client, read, readOpts)
}

func TestRpcMultipleRead(t *testing.T) {
	ctx, read, write, readOpts, host := createCtxReadWriteOpts(t)
	pages := 10
	store := &mockStorage{
		t:        t,
		read:     read,
		write:    write,
		numPages: pages,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client, err := NewGrpcClient(hosts, grpc.WithBlock())
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.NoError(t, err)
	fetch, err := client.Fetch(ctx, read, readOpts)
	require.NoError(t, err)
	checkMultipleRemoteFetch(t, fetch, pages)

	checkWrite(ctx, t, client, write)
}

func TestRpcStopsStreamingWhenFetchKilledOnClient(t *testing.T) {
	ctx, read, write, readOpts, host := createCtxReadWriteOpts(t)
	sleepMillis, numPages := 100, 10
	store := &mockStorage{
		t:           t,
		read:        read,
		write:       write,
		sleepMillis: sleepMillis,
		numPages:    numPages,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client, err := NewGrpcClient(hosts, grpc.WithBlock())
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.NoError(t, err)

	go func() {
		time.Sleep(time.Millisecond * 150)
		readOpts.KillChan <- struct{}{}
	}()
	testStart := time.Now()
	fetch, err := client.Fetch(ctx, read, readOpts)
	testDuration := time.Since(testStart)
	assert.Nil(t, fetch)
	assert.Equal(t, err, m3err.ErrQueryInterrupted)
	assert.True(t, testDuration < time.Duration(sleepMillis*numPages)*time.Millisecond)
}

func TestMultipleClientRpc(t *testing.T) {
	ctx, read, write, readOpts, host := createCtxReadWriteOpts(t)
	store := &mockStorage{
		t:           t,
		read:        read,
		write:       write,
		sleepMillis: 300,
	}
	startServer(t, host, store)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hosts := []string{host}
			client, err := NewGrpcClient(hosts, grpc.WithBlock())
			defer func() {
				err = client.Close()
				assert.NoError(t, err)
			}()
			require.NoError(t, err)

			checkWrite(ctx, t, client, write)
			checkFetch(ctx, t, client, read, readOpts)
		}()
	}

	wg.Wait()
}

type errStorage struct {
	t     *testing.T
	read  *storage.FetchQuery
	write *storage.WriteQuery
}

func (s *errStorage) Fetch(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.FetchResult, error) {
	readQueriesAreEqual(s.t, s.read, query)
	return nil, errRead
}

func (s *errStorage) FetchTags(ctx context.Context, query *storage.FetchQuery, _ *storage.FetchOptions) (*storage.SearchResults, error) {
	return nil, m3err.ErrNotImplemented
}

func (s *errStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	writeQueriesAreEqual(s.t, s.write, query)
	return errWrite
}

func (s *errStorage) FetchBlocks(
	ctx context.Context, query *storage.FetchQuery, options *storage.FetchOptions) (block.Result, error) {
	return block.Result{}, fmt.Errorf("not implemented")
}

func (s *errStorage) Type() storage.Type {
	return storage.Type(-1)
}

func (s *errStorage) Close() error {
	return nil
}

func TestEmptyAddressListErrors(t *testing.T) {
	addresses := []string{}
	client, err := NewGrpcClient(addresses)
	assert.Nil(t, client)
	assert.Equal(t, m3err.ErrNoClientAddresses, err)
}

func TestErrRpc(t *testing.T) {
	ctx, read, write, readOpts, host := createCtxReadWriteOpts(t)
	store := &errStorage{
		t:     t,
		read:  read,
		write: write,
	}
	startServer(t, host, store)
	hosts := []string{host}
	client, err := NewGrpcClient(hosts, grpc.WithBlock())
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.NoError(t, err)

	checkErrorWrite(ctx, t, client, write)
	checkErrorFetch(ctx, t, client, read, readOpts)
}

func TestRoundRobinClientRpc(t *testing.T) {
	ctx, read, write, readOpts, host := createCtxReadWriteOpts(t)
	store := &mockStorage{
		t:     t,
		read:  read,
		write: write,
	}
	startServer(t, host, store)
	errHost := generateAddress()
	errStore := &errStorage{
		t:     t,
		read:  read,
		write: write,
	}
	startServer(t, errHost, errStore)

	hosts := []string{host, errHost}
	client, err := NewGrpcClient(hosts, grpc.WithBlock())
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.NoError(t, err)

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

	hitHost, hitErrHost = false, false
	for i := 0; i < attempts; i++ {
		err := client.Write(ctx, write)
		if err != nil {
			assert.Equal(t, errWrite.Error(), grpc.ErrorDesc(err))
			hitErrHost = true
		} else {
			hitHost = true
		}
		if hitHost && hitErrHost {
			break
		}
	}

	assert.True(t, hitHost, "round robin did not write to host")
	assert.True(t, hitErrHost, "round robin did not write to error host")
}
