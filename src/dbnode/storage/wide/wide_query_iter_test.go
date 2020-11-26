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

package wide

import (
	"io"
	"io/ioutil"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWideQuerySeriesIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pool := encoding.NewMockReaderIteratorPool(ctrl)
	reader := encoding.NewMockReaderIterator(ctrl)
	pool.EXPECT().Get().Return(reader)
	opts := NewOptions().SetReaderIteratorPool(pool)

	singleFinalizeBorrower := func() BorrowedBuffer {
		buf := NewMockBorrowedBuffer(ctrl)
		buf.EXPECT().Finalize()
		return buf
	}

	gomock.InOrder(
		reader.EXPECT().Reset(gomock.Any(), nil).Do(
			func(reader io.Reader, _ namespace.SchemaDescr) {
				read, err := ioutil.ReadAll(reader)
				require.NoError(t, err)
				assert.Equal(t, []byte("data"), read)
			}),

		reader.EXPECT().Next().Return(true),
		reader.EXPECT().Current().Return(ts.Datapoint{Value: 1}, xtime.Second, nil),
		reader.EXPECT().Next().Return(true),
		reader.EXPECT().Current().Return(ts.Datapoint{Value: 2}, xtime.Second, nil),
		reader.EXPECT().Next().Return(false),
		reader.EXPECT().Err().Return(nil),
		reader.EXPECT().Close(),
	)

	iter := newQuerySeriesIterator(opts)
	iter.reset(wideQueryShardIteratorQueuedRecord{
		id:                []byte("id"),
		borrowID:          singleFinalizeBorrower(),
		encodedTags:       []byte("tags"),
		borrowEncodedTags: singleFinalizeBorrower(),
		data:              []byte("data"),
		borrowData:        singleFinalizeBorrower(),
	})

	assert.Equal(t, iter.SeriesMetadata(), SeriesMetadata{
		ID:          ident.BytesID([]byte("id")),
		EncodedTags: ts.EncodedTags([]byte("tags")),
	})

	var vals []float64
	for iter.Next() {
		dp, _, _ := iter.Current()
		vals = append(vals, dp.Value)
	}

	assert.Equal(t, []float64{1, 2}, vals)
	assert.NoError(t, iter.Err())
	iter.Close()
}
