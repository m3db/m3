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

package tile

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/wide"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesBlockIterator(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		opts = Options{
			FrameSize: time.Duration(100),
			Start:     xtime.UnixNano(0),
		}

		seriesIter     = wide.NewMockQuerySeriesIterator(ctrl)
		blockIter      = wide.NewMockCrossBlockIterator(ctrl)
		seriesMetadata = wide.SeriesMetadata{
			ID:          ident.BytesID("foobar"),
			EncodedTags: ts.EncodedTags("encoded tags"),
		}
	)

	gomock.InOrder(
		seriesIter.EXPECT().SeriesMetadata().Return(seriesMetadata),
		seriesIter.EXPECT().Next().Return(true),
		seriesIter.EXPECT().Current().Return(ts.Datapoint{Value: 12}, xtime.Second, nil),
		seriesIter.EXPECT().Next().Return(true),
		seriesIter.EXPECT().Current().Return(ts.Datapoint{Value: 15}, xtime.Second, nil),
		seriesIter.EXPECT().Next().Return(false),
		seriesIter.EXPECT().Err().Return(nil),
	)

	gomock.InOrder(
		blockIter.EXPECT().Next().Return(true),
		blockIter.EXPECT().Current().Return(seriesIter),
		blockIter.EXPECT().Next().Return(false),
		blockIter.EXPECT().Err().Return(nil),
	)

	iter, err := NewSeriesBlockIterator(blockIter, opts)
	require.NoError(t, err)
	assert.True(t, iter.Next())
	frameIter, id, iterTags := iter.Current()
	assert.True(t, frameIter.Next())
	frame := frameIter.Current()

	assert.False(t, frameIter.Next())
	assert.NoError(t, frameIter.Err())
	assert.False(t, iter.Next())
	assert.Equal(t, []float64{12, 15}, frame.Values())

	assert.Equal(t, "foobar", id.String())
	assert.Equal(t, "encoded tags", string(iterTags))
}
