package remote

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"
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
)

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
	read        *storage.ReadQuery
	write       *storage.WriteQuery
	sleepMillis int
}

func (s *mockStorage) Fetch(ctx context.Context, query *storage.ReadQuery) (*storage.FetchResult, error) {
	readQueriesAreEqual(s.t, s.read, query)

	if s.sleepMillis > 0 {
		time.Sleep(time.Millisecond * time.Duration(s.sleepMillis))
	}
	tsSeries := []*ts.Series{makeSeries(ctx)}
	return &storage.FetchResult{
		SeriesList: tsSeries,
		LocalOnly:  false,
	}, nil
}

func (s *mockStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	writeQueriesAreEqual(s.t, s.write, query)
	return nil
}

func (s *mockStorage) Type() storage.Type {
	return storage.Type(0)
}

func checkRemoteFetch(t *testing.T, res *storage.FetchResult) {
	assert.False(t, res.LocalOnly)
	require.Len(t, res.SeriesList, 1)
	s := res.SeriesList[0]
	assert.Equal(t, name, s.Name())
	assert.True(t, startTime.Equal(s.StartTime()))
	assert.Equal(t, tags, s.Tags)
	assert.Equal(t, name, s.Specification)
	assert.Equal(t, len(values), s.Len())
}

func startServer(t *testing.T, store storage.Storage) *grpc.Server {
	server := CreateNewGrpcServer(store)

	go func() {
		err := StartNewGrpcServer(server, "localhost:17762")
		require.Nil(t, err)
	}()
	return server
}

func TestRpc(t *testing.T) {
	ctx := context.Background()
	read, _, _ := createStorageReadQuery(t)
	write, _ := createStorageWriteQuery(t)
	store := &mockStorage{
		t:     t,
		read:  read,
		write: write,
	}
	server := startServer(t, store)
	client, err := NewGrpcClient("localhost:17762")
	require.Nil(t, err)

	fetch, err := client.Fetch(ctx, read)
	require.Nil(t, err)
	checkRemoteFetch(t, fetch)

	err = client.Write(ctx, write)
	require.Equal(t, io.EOF, err)
	server.GracefulStop()
}

func TestMultipleClientRpc(t *testing.T) {
	ctx := context.Background()
	read, _, _ := createStorageReadQuery(t)
	write, _ := createStorageWriteQuery(t)
	store := &mockStorage{
		t:           t,
		read:        read,
		write:       write,
		sleepMillis: 50,
	}
	server := startServer(t, store)

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client, err := NewGrpcClient("localhost:17762")
			require.Nil(t, err)

			fetch, err := client.Fetch(ctx, read)
			require.Nil(t, err)
			checkRemoteFetch(t, fetch)

			err = client.Write(ctx, write)
			require.Equal(t, io.EOF, err)
		}()
	}

	wg.Wait()

	server.GracefulStop()
}

type errStorage struct {
	t     *testing.T
	read  *storage.ReadQuery
	write *storage.WriteQuery
}

func (s *errStorage) Fetch(ctx context.Context, query *storage.ReadQuery) (*storage.FetchResult, error) {
	readQueriesAreEqual(s.t, s.read, query)
	return nil, errRead
}

func (s *errStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	writeQueriesAreEqual(s.t, s.write, query)
	return errWrite
}

func (s *errStorage) Type() storage.Type {
	return storage.Type(-1)
}

func TestErrRpc(t *testing.T) {
	ctx := context.Background()
	read, _, _ := createStorageReadQuery(t)
	write, _ := createStorageWriteQuery(t)
	store := &errStorage{
		t:     t,
		read:  read,
		write: write,
	}
	server := startServer(t, store)
	client, err := NewGrpcClient("localhost:17762")
	require.Nil(t, err)

	fetch, err := client.Fetch(ctx, read)
	assert.Nil(t, fetch)
	assert.Equal(t, errRead.Error(), grpc.ErrorDesc(err))

	err = client.Write(ctx, write)
	assert.Equal(t, errWrite.Error(), grpc.ErrorDesc(err))
	server.GracefulStop()
}
