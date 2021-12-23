// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/m3ninx/doc"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestWriteBatchSortByUnmarkedAndIndexBlockStart(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	blockSize := time.Hour

	now := xtime.Now()
	blockStart := now.Truncate(blockSize)

	nowNotBlockStartAligned := now.
		Truncate(blockSize).
		Add(time.Minute)

	h1 := doc.NewMockOnIndexSeries(ctrl)
	h1.EXPECT().OnIndexFinalize(blockStart)
	h1.EXPECT().OnIndexSuccess(blockStart)

	h2 := doc.NewMockOnIndexSeries(ctrl)
	h2.EXPECT().OnIndexFinalize(blockStart)

	h3 := doc.NewMockOnIndexSeries(ctrl)
	h3.EXPECT().OnIndexFinalize(blockStart)
	h3.EXPECT().OnIndexSuccess(blockStart)

	batch := NewWriteBatch(WriteBatchOptions{
		IndexBlockSize: blockSize,
	})
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h1,
	}, testDoc1())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h2,
	}, testDoc2())
	batch.Append(WriteBatchEntry{
		Timestamp:     nowNotBlockStartAligned,
		OnIndexSeries: h3,
	}, testDoc3())

	// Mark entry in middle as failure
	batch.MarkUnmarkedEntryError(fmt.Errorf("an error"), 1)

	// Now sort by unmarked and block start.
	batch.SortByUnmarkedAndIndexBlockStart()

	// Make sure two remaining.
	require.Equal(t, 2, len(batch.PendingDocs()))

	// Make sure marks two done.
	batch.MarkUnmarkedEntriesSuccess()

	// Make sure none remaining.
	require.Equal(t, 0, len(batch.PendingDocs()))
}
