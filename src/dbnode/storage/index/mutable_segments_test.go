// Copyright (c) 2021 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	xsync "github.com/m3db/m3/src/x/sync"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
)

func newTestMutableSegments(
	t *testing.T,
	md namespace.Metadata,
	blockStart time.Time,
) *mutableSegments {
	cachedSearchesWorkers := xsync.NewWorkerPool(2)
	cachedSearchesWorkers.Init()

	segs, err := newMutableSegments(md, blockStart, testOpts, BlockOptions{},
		cachedSearchesWorkers, namespace.NewRuntimeOptionsManager("foo"),
		testOpts.InstrumentOptions())
	require.NoError(t, err)

	return segs
}

func TestMutableSegmentsBackgroundCompact(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour
	testMD := newTestNSMetadata(t)
	blockStart := time.Now().Truncate(blockSize)

	nowNotBlockStartAligned := blockStart.Add(time.Minute)

	segs := newTestMutableSegments(t, testMD, blockStart)
	segs.backgroundCompactDisable = true // Disable to explicitly test.

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})

	for i := 0; i < 32; i++ {
		batch.Append(WriteBatchEntry{
			Timestamp: nowNotBlockStartAligned,
		}, testDocN(i))
	}

	_, err := segs.WriteBatch(batch)
	require.NoError(t, err)
}
