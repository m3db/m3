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

package ts

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

const (
	batchSize    = 2
	maxBatchSize = 10
)

var (
	namespace = ident.StringID("namespace")
	numShards = 10
	shardFn   = sharding.NewHashFn(numShards, 0)
)

type testWrite struct {
	id         ident.ID
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

func TestBatchWriter(t *testing.T) {
	var (
		batchWriter = NewWriteBatch(batchSize, maxBatchSize, namespace, shardFn)
		writes      = []testWrite{
			{
				id:         ident.StringID("series1"),
				timestamp:  time.Now(),
				value:      0,
				unit:       xtime.Nanosecond,
				annotation: []byte("annotation1"),
			},
			{
				id:         ident.StringID("series2"),
				timestamp:  time.Now(),
				value:      1,
				unit:       xtime.Nanosecond,
				annotation: []byte("annotation2s"),
			},
		}
	)

	for _, write := range writes {
		batchWriter.Add(
			write.id,
			write.timestamp,
			write.value,
			write.unit,
			write.annotation)
	}

	// Make sure they're sorted
	var (
		iter      = batchWriter.Iter()
		lastShard = uint32(0)
	)
	for iter.Next() {
		var (
			curr      = iter.Current()
			currShard = curr.Write.Series.Shard
		)

		require.True(t, currShard >= lastShard)
		lastShard = currShard
	}

	// Make sure all the data is there
	for _, write := range writes {
		iter := batchWriter.Iter()
		for iter.Next() {
			currSeries := iter.Current().Write.Series
			if currSeries.ID.Equal(write.id) {

				continue
			}

		}
	}

}
