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

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/sharding"
	"github.com/spaolacci/murmur3"
	"github.com/stretchr/testify/require"
)

func testShardingScheme(t *testing.T) m3db.ShardScheme {
	shardScheme, err := sharding.NewShardScheme(0, 1, func(id string) uint32 {
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

func testDatabase(t *testing.T) *db {
	ss := testShardingScheme(t)
	opts := testDatabaseOptions()
	return NewDatabase(ss.All(), opts).(*db)
}

func TestDatabaseOpen(t *testing.T) {
	d := testDatabase(t)
	require.NoError(t, d.Open())
	require.Equal(t, errDatabaseAlreadyOpen, d.Open())
}

func TestDatabaseClose(t *testing.T) {
	d := testDatabase(t)
	require.NoError(t, d.Open())
	require.NoError(t, d.Close())
	require.Equal(t, errDatabaseAlreadyClosed, d.Close())
}
