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

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSeriesBlockIterator(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	it := encoding.NewMockReaderIterator(ctrl)
	start := time.Now().Truncate(time.Hour)

	it.EXPECT().Err().Return(nil)
	it.EXPECT().Reset(gomock.Any(), nil)
	it.EXPECT().Next().Return(true)
	it.EXPECT().Current().Return(ts.Datapoint{
		Timestamp: start,
		Value:     1,
	}, xtime.Second, nil)
	it.EXPECT().Next().Return(false)

	iterPool := encoding.NewMockReaderIteratorPool(ctrl)
	iterPool.EXPECT().Get().Return(it)

	opts := Options{
		FrameSize:          xtime.UnixNano(100),
		Start:              xtime.UnixNano(0),
		UseArrow:           false,
		ReaderIteratorPool: iterPool,
	}

	reader := fs.NewMockCrossBlockReader(ctrl)
	reader.EXPECT().Next().Return(true)
	tags := ident.MustNewTagStringsIterator("foo", "bar")
	checkedBytes := checked.NewBytes([]byte("foobar"), checked.NewBytesOptions())
	records := []fs.BlockRecord{fs.BlockRecord{Data: checkedBytes}}
	reader.EXPECT().Current().Return(ident.StringID("foobar"), tags, records)
	reader.EXPECT().Next().Return(false)
	reader.EXPECT().Err().Return(nil)

	iter, err := NewSeriesBlockIterator(reader, opts)
	require.NoError(t, err)
	assert.True(t, iter.Next())
	frameIter, id, iterTags := iter.Current()
	assert.True(t, frameIter.Next())
	frame := frameIter.Current()
	assert.False(t, frameIter.Next())
	assert.False(t, iter.Next())
	assert.Equal(t, 1.0, frame.Sum())

	assert.Equal(t, "foobar", id.String())
	require.True(t, iterTags.Next())
	assert.Equal(t, "foo", iterTags.Current().Name.String())
	assert.Equal(t, "bar", iterTags.Current().Value.String())
	assert.False(t, iterTags.Next())
}
