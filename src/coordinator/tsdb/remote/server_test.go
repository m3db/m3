package remote

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	m3err "github.com/m3db/m3coordinator/errors"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

const (
	name = "aa"
	mps  = 5
)

var (
	startTime, _ = time.Parse(time.RFC3339, "2000-02-06T11:54:48+07:00")
	tags         = models.Tags{"1": "b", "2": "c"}
	values       = []float64{1.0, 2.0, 3.0, 4.0}
	errWrite     = errors.New("write error")
	errRead      = errors.New("read error")
	initialPort  = 17762
	testMu       sync.Mutex
)

func generateAddress() string {
	testMu.Lock()
	defer testMu.Unlock()
	address := fmt.Sprintf("localhost:%d", initialPort)
	initialPort++
	return address
}

func makeValues(ctx context.Context) ts.Values {
	vals := ts.NewValues(ctx, mps, len(values))
	for i, v := range values {
		vals.SetValueAt(i, v)
	}
	return vals
}

func makeSeries(ctx context.Context) *ts.Series {
	return ts.NewSeries(ctx, name, startTime, makeValues(ctx), tags)
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

	tsSeries := []*ts.Series{makeSeries(ctx)}
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

func checkMultipleRemoteFetch(t *testing.T, res *storage.FetchResult, numResults int) {
	assert.False(t, res.LocalOnly)
	require.Len(t, res.SeriesList, numResults)
	for _, s := range res.SeriesList {
		assert.Equal(t, name, s.Name())
		assert.True(t, startTime.Equal(s.StartTime()))
		assert.Equal(t, tags, s.Tags)
		assert.Equal(t, name, s.Specification)
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

func (s *errStorage) Type() storage.Type {
	return storage.Type(-1)
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

	hosts := []string{errHost, host}
	client, err := NewGrpcClient(hosts, grpc.WithBlock())
	defer func() {
		err = client.Close()
		assert.NoError(t, err)
	}()
	require.NoError(t, err)

	// Host ordering is non deterministic. Ensure round robin occurs after first result.
	fetch, err := client.Fetch(ctx, read, readOpts)
	if fetch == nil {
		// Fetch called on errHost
		assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))

		// Write called on host
		checkWrite(ctx, t, client, write)

		// Write called on errHost
		checkErrorWrite(ctx, t, client, write)

		// Fetch called on host
		checkFetch(ctx, t, client, read, readOpts)
	} else {
		//Fetch called on host
		checkRemoteFetch(t, fetch)

		// Fetch called on errHost
		checkErrorFetch(ctx, t, client, read, readOpts)

		// Write called on hostß
		checkWrite(ctx, t, client, write)

		// Write called on errHost
		checkErrorWrite(ctx, t, client, write)
	}
}
