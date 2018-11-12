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
	"bytes"
	"fmt"
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
	writes    = []testWrite{
		{
			id: ident.StringID("series1"),
			tagIter: ident.NewTagsIterator(ident.NewTags(
				ident.Tag{
					Name:  ident.StringID("name1"),
					Value: ident.StringID("value1"),
				})),
			timestamp:  time.Now(),
			value:      0,
			unit:       xtime.Nanosecond,
			annotation: []byte("annotation1"),
		},
		{
			id: ident.StringID("series2"),
			tagIter: ident.NewTagsIterator(ident.NewTags(
				ident.Tag{
					Name:  ident.StringID("name2"),
					Value: ident.StringID("value2"),
				})),
			timestamp:  time.Now(),
			value:      1,
			unit:       xtime.Nanosecond,
			annotation: []byte("annotation2s"),
		},
	}
)

type testWrite struct {
	id         ident.ID
	tagIter    ident.TagIterator
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

func TestBatchWriterAddAndIter(t *testing.T) {
	writeBatch := NewWriteBatch(batchSize, namespace, nil)

	for i, write := range writes {
		writeBatch.Add(
			i,
			write.id,
			write.timestamp,
			write.value,
			write.unit,
			write.annotation)
	}

	// Make sure all the data is there
	assertDataPresent(t, writes, writeBatch)
}

func TestBatchWriterAddTaggedAndIter(t *testing.T) {
	writeBatch := NewWriteBatch(batchSize, namespace, nil)

	for i, write := range writes {
		writeBatch.AddTagged(
			i,
			write.id,
			write.tagIter,
			write.timestamp,
			write.value,
			write.unit,
			write.annotation)
	}

	// Make sure all the data is there
	assertDataPresent(t, writes, writeBatch)
}

func TestBatchWriterSetSeries(t *testing.T) {
	writeBatch := NewWriteBatch(batchSize, namespace, nil)

	for i, write := range writes {
		writeBatch.AddTagged(
			i,
			write.id,
			write.tagIter,
			write.timestamp,
			write.value,
			write.unit,
			write.annotation)
	}

	// Update the series
	iter := writeBatch.Iter()
	for i, curr := range iter {
		var (
			currWrite  = curr.Write
			currSeries = currWrite.Series
			newSeries  = currSeries
		)
		newSeries.ID = ident.StringID(string(i))

		writeBatch.SetSeries(i, newSeries)
	}

	// Assert the series have been updated
	iter = writeBatch.Iter()
	for i, curr := range iter {
		var (
			currWrite  = curr.Write
			currSeries = currWrite.Series
		)
		require.True(t, ident.StringID(string(i)).Equal(currSeries.ID))
	}
}

func TestWriteBatchReset(t *testing.T) {
	var (
		numResets  = 10
		writeBatch = NewWriteBatch(batchSize, namespace, nil)
	)

	for i := 0; i < numResets; i++ {
		writeBatch.Reset(batchSize, namespace, shardFn)
		for _, write := range writes {
			writeBatch.Add(
				i,
				write.id,
				write.timestamp,
				write.value,
				write.unit,
				write.annotation)
		}

		// Make sure all the data is there
		assertDataPresent(t, writes, writeBatch)
	}
}

func assertDataPresent(t *testing.T, writes []testWrite, batchWriter WriteBatch) {
	for _, write := range writes {
		var (
			iter  = batchWriter.Iter()
			found = false
		)

		for _, currWriteBatch := range iter {
			var (
				currWrite  = currWriteBatch.Write
				currSeries = currWrite.Series
			)

			if currSeries.ID.Equal(write.id) {
				require.Equal(t, namespace, currWrite.Series.Namespace)
				require.Equal(t, write.timestamp, currWrite.Datapoint.Timestamp)
				require.Equal(t, write.value, currWrite.Datapoint.Value)
				require.Equal(t, write.unit, currWrite.Unit)
				require.True(t, bytes.Equal(write.annotation, currWrite.Annotation))
				found = true
				break
			}
		}

		require.True(t, found, fmt.Sprintf("expected to find series: %s", write.id))
	}
}
