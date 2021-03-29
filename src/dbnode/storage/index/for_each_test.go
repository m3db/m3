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

package index

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/m3ninx/doc"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestWriteBatchForEachUnmarkedBatchByBlockStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	now := time.Now().Truncate(blockSize)
	tn := func(n int64) time.Time {
		nDur := time.Duration(n)
		return now.Add(nDur * blockSize).Add(nDur * time.Minute)
	}
	d := func(n int64) doc.Metadata {
		return doc.Metadata{
			ID: []byte(fmt.Sprintf("doc-%d", n)),
		}
	}
	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	for _, n := range []int64{2, 0, 1} {
		batch.Append(WriteBatchEntry{
			Timestamp:     tn(n),
			OnIndexSeries: NewMockOnIndexSeries(ctrl),
		}, d(n))
	}

	numCalls := 0
	// entries.ForEachBlockStart(func(ts xtime.UnixNano, writes []WriteBatchEntry) {
	batch.ForEachUnmarkedBatchByBlockStart(func(
		blockStart time.Time,
		batch *WriteBatch,
	) {
		require.Equal(t, 1, batch.Len())
		switch numCalls {
		case 0:
			require.Equal(t, "doc-0", string(batch.PendingDocs()[0].ID))
		case 1:
			require.Equal(t, "doc-1", string(batch.PendingDocs()[0].ID))
		case 2:
			require.Equal(t, "doc-2", string(batch.PendingDocs()[0].ID))
		default:
			require.FailNow(t, "should never get here")
		}
		numCalls++
	})
	require.Equal(t, 3, numCalls)
}

func TestWriteBatchForEachUnmarkedBatchByBlockStartMore(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	now := time.Now().Truncate(blockSize)
	tn := func(n int64) time.Time {
		nDur := time.Duration(n)
		return now.Add(nDur * blockSize).Add(nDur * time.Minute)
	}
	d := func(n int64) doc.Metadata {
		return doc.Metadata{
			ID: []byte(fmt.Sprintf("doc-%d", n)),
		}
	}
	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	for _, v := range []struct {
		nTime int64
		nDoc  int64
	}{
		{0, 0},
		{1, 3},
		{0, 1},
		{1, 4},
		{0, 2},
	} {
		batch.Append(WriteBatchEntry{
			Timestamp:     tn(v.nTime),
			OnIndexSeries: NewMockOnIndexSeries(ctrl),
		}, d(v.nDoc))
	}

	numCalls := 0
	batch.ForEachUnmarkedBatchByBlockStart(func(
		blockStart time.Time,
		batch *WriteBatch,
	) {
		switch numCalls {
		case 0:
			require.Equal(t, 3, batch.Len())
			require.Equal(t, "doc-0", string(batch.PendingDocs()[0].ID))
			require.Equal(t, "doc-1", string(batch.PendingDocs()[1].ID))
			require.Equal(t, "doc-2", string(batch.PendingDocs()[2].ID))
		case 1:
			require.Equal(t, 2, batch.Len())
			require.Equal(t, "doc-3", string(batch.PendingDocs()[0].ID))
			require.Equal(t, "doc-4", string(batch.PendingDocs()[1].ID))
		default:
			require.FailNow(t, "should never get here")
		}
		numCalls++
	})
	require.Equal(t, 2, numCalls)
}
