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
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"
	"github.com/m3db/m3db/sharding"

	"github.com/golang/mock/gomock"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func testShardingScheme(t *testing.T) m3db.ShardScheme {
	shardScheme, err := sharding.NewShardScheme(0, 1023, func(id string) uint32 {
		return murmur3.Sum32([]byte(id)) % 1024
	})
	require.NoError(t, err)
	return shardScheme
}

func testDatabaseOptions() m3db.DatabaseOptions {
	var opts m3db.DatabaseOptions
	opts = NewDatabaseOptions().
		NowFn(time.Now).
		BufferFuture(10 * time.Minute).
		BufferPast(10 * time.Minute).
		BufferDrain(10 * time.Minute).
		BlockSize(2 * time.Hour).
		RetentionPeriod(2 * 24 * time.Hour).
		MaxFlushRetries(3)
	return opts
}

func testDatabase(t *testing.T, bs bootstrapState) *db {
	ss := testShardingScheme(t)
	opts := testDatabaseOptions()
	d := NewDatabase(ss.CreateSet(397, 397), opts).(*db)
	bsm := newBootstrapManager(d).(*bootstrapManager)
	bsm.state = bs
	d.bsm = bsm
	return d
}

func TestDatabaseOpen(t *testing.T) {
	d := testDatabase(t, bootstrapNotStarted)
	require.NoError(t, d.Open())
	require.Equal(t, errDatabaseAlreadyOpen, d.Open())
}

func TestDatabaseClose(t *testing.T) {
	d := testDatabase(t, bootstrapped)
	require.NoError(t, d.Open())
	require.NoError(t, d.Close())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}

func TestDatabaseReadEncodedNotBootstrapped(t *testing.T) {
	d := testDatabase(t, bootstrapNotStarted)
	_, err := d.ReadEncoded(context.NewContext(), "foo", time.Now(), time.Now())
	require.Equal(t, errDatabaseNotBootstrapped, err)
}

func TestDatabaseReadEncodedShardNotOwned(t *testing.T) {
	d := testDatabase(t, bootstrapped)
	_, err := d.ReadEncoded(context.NewContext(), "foo", time.Now(), time.Now())
	require.Equal(t, "not responsible for shard 32", err.Error())
	require.Panics(t, func() { d.RUnlock() }, "shouldn't be able to unlock the read lock")
}

func TestDatabaseReadEncodedShardOwned(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := testDatabase(t, bootstrapped)
	ctx := context.NewContext()
	id := "bar"
	end := time.Now()
	start := end.Add(-time.Hour)
	mockShard := mocks.NewMockdatabaseShard(ctrl)
	mockShard.EXPECT().ReadEncoded(ctx, id, start, end).Return(nil, nil)
	d.shards[397] = mockShard
	res, err := d.ReadEncoded(ctx, id, start, end)
	require.Nil(t, res)
	require.Nil(t, err)
	require.Panics(t, func() { d.RUnlock() }, "shouldn't be able to unlock the read lock")
}
