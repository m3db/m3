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

package writes

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

const (
	batchSize = 2
)

var (
	namespace = ident.StringID("namespace")
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
		{
			id: ident.StringID("series3"),
			tagIter: ident.NewTagsIterator(ident.NewTags(
				ident.Tag{
					Name:  ident.StringID("name3"),
					Value: ident.StringID("value3"),
				})),
			timestamp:  time.Now(),
			value:      2,
			unit:       xtime.Nanosecond,
			annotation: []byte("annotation3s"),
		},
	}
)

var (
	testTagEncodingPool = struct {
		once sync.Once
		pool serialize.TagEncoderPool
	}{
		pool: serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
			pool.NewObjectPoolOptions().SetSize(1)),
	}
)

func getTagEncoder() serialize.TagEncoder {
	testTagEncodingPool.once.Do(func() {
		testTagEncodingPool.pool.Init()
	})
	return testTagEncodingPool.pool.Get()
}

type testWrite struct {
	id         ident.ID
	tagIter    ident.TagIterator
	timestamp  time.Time
	value      float64
	unit       xtime.Unit
	annotation []byte
}

func (w testWrite) encodedTags(t *testing.T) checked.Bytes {
	encoder := getTagEncoder()
	require.NoError(t, encoder.Encode(w.tagIter.Duplicate()))
	data, ok := encoder.Data()
	require.True(t, ok)
	return data
}

func TestBatchWriterAddAndIter(t *testing.T) {
	writeBatch := NewWriteBatch(namespace, nil)

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
	writeBatch := NewWriteBatch(namespace, nil)

	for i, write := range writes {
		writeBatch.AddTagged(
			i,
			write.id,
			write.tagIter,
			write.encodedTags(t).Bytes(),
			write.timestamp,
			write.value,
			write.unit,
			write.annotation)
	}

	// Make sure all the data is there
	assertDataPresent(t, writes, writeBatch)
}

func TestBatchWriterSetSeries(t *testing.T) {
	writeBatch := NewWriteBatch(namespace, nil)

	for i, write := range writes {
		writeBatch.AddTagged(
			i,
			write.id,
			write.tagIter,
			write.encodedTags(t).Bytes(),
			write.timestamp,
			write.value,
			write.unit,
			write.annotation)
	}

	// Set the outcome
	iter := writeBatch.Iter()
	for i, curr := range iter {
		if i == 0 {
			// skip the first write.
			writeBatch.SetSkipWrite(i)
			continue
		}

		var (
			currWrite  = curr.Write
			currSeries = currWrite.Series
			newSeries  = currSeries
		)
		newSeries.ID = ident.StringID(fmt.Sprint(i))

		if i == len(iter)-1 {
			// Set skip for this to true; it should revert to not skipping after
			// SetOutcome called below.
			err := errors.New("some-error")
			writeBatch.SetError(i, err)
		} else {
			writeBatch.SetSeries(i, newSeries)
		}
	}

	iter = writeBatch.Iter()
	require.Equal(t, 3, len(iter))

	require.True(t, iter[0].SkipWrite)

	for j, curr := range iter[1:] {
		var (
			currWrite  = curr.Write
			currSeries = currWrite.Series
			i          = j + 1
		)
		if i == len(iter)-1 {
			require.Equal(t, errors.New("some-error"), curr.Err)
			require.True(t, curr.SkipWrite)
			continue
		}

		require.Equal(t, fmt.Sprint(i), string(currSeries.ID.String()))
		require.False(t, curr.SkipWrite)

		require.NoError(t, curr.Err)
	}
}

func TestWriteBatchReset(t *testing.T) {
	var (
		numResets  = 10
		writeBatch = NewWriteBatch(namespace, nil)
	)

	for i := 0; i < numResets; i++ {
		writeBatch.Reset(batchSize, namespace)
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

func TestBatchWriterFinalizer(t *testing.T) {
	var (
		numEncodedTagsFinalized = 0
		numAnnotationsFinalized = 0
		numFinalized            = 0

		finalizeEncodedTagsFn = func(b []byte) {
			numEncodedTagsFinalized++
		}
		finalizeAnnotationFn = func(b []byte) {
			numAnnotationsFinalized++
		}
		finalizeFn = func(b WriteBatch) {
			numFinalized++
		}
	)

	writeBatch := NewWriteBatch(namespace, finalizeFn)
	writeBatch.SetFinalizeEncodedTagsFn(finalizeEncodedTagsFn)
	writeBatch.SetFinalizeAnnotationFn(finalizeAnnotationFn)

	for i, write := range writes {
		writeBatch.AddTagged(
			i,
			write.id,
			write.tagIter,
			write.encodedTags(t).Bytes(),
			write.timestamp,
			write.value,
			write.unit,
			write.annotation)
	}

	require.Equal(t, 3, len(writeBatch.Iter()))
	writeBatch.Finalize()
	require.Equal(t, 0, len(writeBatch.Iter()))
	require.Equal(t, 1, numFinalized)
	require.Equal(t, 3, numEncodedTagsFinalized)
	require.Equal(t, 3, numAnnotationsFinalized)
}
