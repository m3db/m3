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
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3x/errors"

	"github.com/golang/mock/gomock"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func testShardSet(t *testing.T) sharding.ShardSet {
	shardSet, err := sharding.NewShardSet([]uint32{397}, func(id string) uint32 {
		return murmur3.Sum32([]byte(id)) % 1024
	})
	require.NoError(t, err)
	return shardSet
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
	ss := testShardSet(t)
	opts := testDatabaseOptions()
	database, err := NewDatabase(ss, opts)
	require.NoError(t, err)
	d := database.(*db)
	bsm := newBootstrapManager(d).(*bootstrapManager)
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
	_, err := d.ReadEncoded(ctx, "foo", time.Now(), time.Now())
	require.Equal(t, errDatabaseNotBootstrapped, err)
}

func TestDatabaseReadEncodedShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	_, err := d.ReadEncoded(ctx, "foo", time.Now(), time.Now())
	require.Equal(t, "not responsible for shard 32", err.Error())
	require.Panics(t, func() { d.RUnlock() }, "shouldn't be able to unlock the read lock")
}

func TestDatabaseReadEncodedShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	id := "bar"
	end := time.Now()
	start := end.Add(-time.Hour)
	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ReadEncoded(ctx, id, start, end).Return(nil, nil)
	d.shards[397] = mockShard
	res, err := d.ReadEncoded(ctx, id, start, end)
	require.Nil(t, res)
	require.Nil(t, err)
	require.Panics(t, func() { d.RUnlock() }, "shouldn't be able to unlock the read lock")
}

func TestDatabaseFetchBlocksShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	expected := []time.Time{starts[2], starts[0], starts[1]}
	res := d.FetchBlocks(ctx, "foo", starts)
	require.Equal(t, len(expected), len(res))
	for i := 0; i < len(expected); i++ {
		require.Equal(t, expected[i], res[i].Start())
		require.Nil(t, res[i].Readers())
		require.True(t, xerrors.IsInvalidParams(res[i].Error()))
	}
}

func TestDatabaseFetchBlocksShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	id := "bar"
	now := time.Now()
	starts := []time.Time{now, now.Add(time.Second), now.Add(-time.Second)}
	expected := []FetchBlockResult{newFetchBlockResult(starts[0], nil, nil)}
	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().FetchBlocks(ctx, id, starts).Return(expected)
	d.shards[397] = mockShard
	res := d.FetchBlocks(ctx, id, starts)
	require.Equal(t, expected, res)
}

func TestDatabaseFetchBlocksMetadataShardNotOwned(t *testing.T) {
	ctx := context.NewContext()
	defer ctx.Close()

	d := testDatabase(t, bootstrapped)
	shardID, limit, pageToken, includeSizes := uint32(0), int64(100), int64(0), true
	res, nextPageToken, err := d.FetchBlocksMetadata(ctx, shardID, limit, pageToken, includeSizes)
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
	shardID, limit, pageToken, includeSizes := uint32(397), int64(100), int64(0), true
	expectedBlocks := []block.DatabaseBlocksMetadata{block.NewDatabaseBlocksMetadata("bar", nil)}
	expectedToken := new(int64)
	mockShard := NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().FetchBlocksMetadata(ctx, limit, pageToken, includeSizes).Return(expectedBlocks, expectedToken, nil)
	d.shards[int(shardID)] = mockShard
	res, nextToken, err := d.FetchBlocksMetadata(ctx, shardID, limit, pageToken, includeSizes)
	require.Equal(t, expectedBlocks, res)
	require.Equal(t, expectedToken, nextToken)
	require.Nil(t, err)
}
