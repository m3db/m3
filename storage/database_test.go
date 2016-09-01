// Copyright (c) 2016 Uber Technologies, Inc.
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

package storage

import (
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type mockDatabase struct {
	opts       Options
	namespaces map[string]databaseNamespace
	bs         bootstrapState
}

func newMockDatabase() *mockDatabase                             { return &mockDatabase{opts: testDatabaseOptions()} }
func (d *mockDatabase) Options() Options                         { return d.opts }
func (d *mockDatabase) Open() error                              { return nil }
func (d *mockDatabase) Close() error                             { return nil }
func (d *mockDatabase) Bootstrap() error                         { return nil }
func (d *mockDatabase) IsBootstrapped() bool                     { return d.bs == bootstrapped }
func (d *mockDatabase) TruncateNamespace(namespace string) error { return nil }
func (d *mockDatabase) flush(t time.Time, async bool)            {}

func (d *mockDatabase) getOwnedNamespaces() []databaseNamespace {
	namespaces := make([]databaseNamespace, 0, len(d.namespaces))
	for _, n := range d.namespaces {
		namespaces = append(namespaces, n)
	}
	return namespaces
}

func (d *mockDatabase) Write(context.Context, string, string, time.Time, float64, xtime.Unit, []byte) error {
	return nil
}

func (d *mockDatabase) ReadEncoded(context.Context, string, string, time.Time, time.Time) ([][]xio.SegmentReader, error) {
	return nil, nil
}

func (d *mockDatabase) FetchBlocks(context.Context, string, uint32, string, []time.Time) ([]FetchBlockResult, error) {
	return nil, nil
}

func (d *mockDatabase) FetchBlocksMetadata(context.Context, string, uint32, int64, int64, bool) ([]FetchBlocksMetadataResult, *int64, error) {
	return nil, nil, nil
}

func testDatabaseOptions() Options {
	return NewOptions().
		MaxFlushRetries(3).
		RetentionOptions(retention.NewOptions().
			BufferFuture(10 * time.Minute).
			BufferPast(10 * time.Minute).
			BufferDrain(10 * time.Minute).
			BlockSize(2 * time.Hour).
			RetentionPeriod(2 * 24 * time.Hour))
}

func testDatabase(t *testing.T, bs bootstrapState) *db {
	opts := testDatabaseOptions()
	database, err := NewDatabase(nil, nil, opts)
	require.NoError(t, err)
	d := database.(*db)
	bsm := newBootstrapManager(d, nil).(*bootstrapManager)
	bsm.state = bs
	d.bsm = bsm
	return d
}

func TestDatabaseOpen(t *testing.T) {
	d := testDatabase(t, bootstrapNotStarted)
	require.NoError(t, d.Open())
	require.Equal(t, errDatabaseAlreadyOpen, d.Open())
	require.NoError(t, d.Close())
}

func TestDatabaseClose(t *testing.T) {
	d := testDatabase(t, bootstrapped)
	require.NoError(t, d.Open())
	require.NoError(t, d.Close())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}

func TestDatabaseReadEncodedNotBootstrapped(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapNotStarted)
	_, err := d.ReadEncoded(ctx, "testns1", "foo", time.Now(), time.Now())
	require.Equal(t, errDatabaseNotBootstrapped, err)
}

func TestDatabaseReadEncodedNamespaceNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	_, err := d.ReadEncoded(ctx, "nonexistent", "foo", time.Now(), time.Now())
	require.Equal(t, "not responsible for namespace nonexistent", err.Error())
	require.Panics(t, func() { d.RUnlock() }, "shouldn't be able to unlock the read lock")
}

func TestDatabaseReadEncodedNamespaceOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	ns := "testns1"
	id := "bar"
	end := time.Now()
	start := end.Add(-time.Hour)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().ReadEncoded(ctx, id, start, end).Return(nil, nil)
	d.namespaces[ns] = mockNamespace

	res, err := d.ReadEncoded(ctx, ns, id, start, end)
	require.Nil(t, res)
	require.Nil(t, err)
	require.Panics(t, func() { d.RUnlock() }, "shouldn't be able to unlock the read lock")
}

func TestDatabaseFetchBlocksNamespaceNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	res, err := d.FetchBlocks(ctx, "testns1", 0, "foo", starts)
	require.Nil(t, res)
	require.True(t, xerrors.IsInvalidParams(err))
}

func TestDatabaseFetchBlocksNamespaceOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	ns := "testns1"
	id := "bar"
	shardID := uint32(0)
	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	expected := []FetchBlockResult{newFetchBlockResult(starts[0], nil, nil)}
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().FetchBlocks(ctx, shardID, id, starts).Return(expected, nil)
	d.namespaces[ns] = mockNamespace

	res, err := d.FetchBlocks(ctx, ns, shardID, id, starts)
	require.Equal(t, expected, res)
	require.NoError(t, err)
}

func TestDatabaseFetchBlocksMetadataShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	ns, shardID, limit, pageToken, includeSizes := "testns1", uint32(0), int64(100), int64(0), true
	res, nextPageToken, err := d.FetchBlocksMetadata(ctx, ns, shardID, limit, pageToken, includeSizes)
	require.Nil(t, res)
	require.Nil(t, nextPageToken)
	require.Error(t, err)
}

func TestDatabaseFetchBlocksMetadataShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	ns, shardID, limit, pageToken, includeSizes := "testns1", uint32(397), int64(100), int64(0), true
	expectedBlocks := []FetchBlocksMetadataResult{newFetchBlocksMetadataResult("bar", nil)}
	expectedToken := new(int64)
	mockNamespace := NewMockdatabaseNamespace(ctrl)
	mockNamespace.EXPECT().FetchBlocksMetadata(ctx, shardID, limit, pageToken, includeSizes).Return(expectedBlocks, expectedToken, nil)
	d.namespaces[ns] = mockNamespace

	res, nextToken, err := d.FetchBlocksMetadata(ctx, ns, shardID, limit, pageToken, includeSizes)
	require.Equal(t, expectedBlocks, res)
	require.Equal(t, expectedToken, nextToken)
	require.Nil(t, err)
}
