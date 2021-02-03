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

package bootstrapper

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestBootstrapIndexSegmentOutOfRetention(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	blockSize := 2 * time.Hour
	retentionPeriod := 4 * time.Hour
	now := time.Now().Add(-retentionPeriod)
	start := now.Add(-retentionPeriod).Truncate(blockSize)
	end := start.Add(retentionPeriod)
	shardTimeRanges := result.NewShardTimeRanges().Set(
		0,
		xtime.NewRanges(xtime.Range{
			Start: start,
			End:   end,
		}),
	)

	rOpts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(retentionPeriod)

	nsOptions := namespace.NewOptions().SetRetentionOptions(rOpts)

	mockMetadata := namespace.NewMockMetadata(ctrl)
	mockMetadata.EXPECT().Options().Return(nsOptions).AnyTimes()

	builder := segment.NewMockCloseableDocumentsBuilder(ctrl)
	metadata := make([]doc.Metadata, 10)
	builder.EXPECT().Docs().Return(metadata).AnyTimes()

	resultOptions := result.NewOptions()
	resultOptions.ClockOptions().SetNowFn(func() time.Time {
		return now
	})

	blockStart := start
	blockEnd := end // blockEnd equals earliest retention time

	_, err := PersistBootstrapIndexSegment(
		mockMetadata,
		shardTimeRanges,
		builder,
		nil,
		nil,
		resultOptions,
		shardTimeRanges,
		blockStart,
		blockEnd)

	require.Equal(t, fs.ErrIndexOutOfRetention, err)

	_, err = BuildBootstrapIndexSegment(
		mockMetadata,
		shardTimeRanges,
		builder,
		nil,
		resultOptions,
		nil,
		blockStart,
		blockEnd)
	require.Equal(t, fs.ErrIndexOutOfRetention, err)

	blockEnd = end.Add(-1 * time.Hour) // blockEnd less than earliest retention time

	_, err = PersistBootstrapIndexSegment(
		mockMetadata,
		shardTimeRanges,
		builder,
		nil,
		nil,
		resultOptions,
		shardTimeRanges,
		blockStart,
		blockEnd)

	require.Equal(t, fs.ErrIndexOutOfRetention, err)

	_, err = BuildBootstrapIndexSegment(
		mockMetadata,
		shardTimeRanges,
		builder,
		nil,
		resultOptions,
		nil,
		blockStart,
		blockEnd)
	require.Equal(t, fs.ErrIndexOutOfRetention, err)
}
