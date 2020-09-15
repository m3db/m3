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

package fs

import (
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCrossBlockIterator(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	reader := encoding.NewMockReaderIterator(ctrl)

	iterPool := encoding.NewMockReaderIteratorPool(ctrl)
	iterPool.EXPECT().Get().Return(reader)

	iter := NewCrossBlockIterator(iterPool)
	assert.False(t, iter.Next())

	count := 3
	iterCount := 5
	startTime := time.Now().Truncate(time.Hour)
	start := startTime
	records := make([]BlockRecord, 0, count)
	for i := 0; i < count; i++ {
		byteString := fmt.Sprint(i)
		records = append(records, BlockRecord{
			Data: []byte(byteString),
		})

		reader.EXPECT().Reset(gomock.Any(), nil).Do(
			func(r io.Reader, _ namespace.SchemaDescr) {
				b, err := ioutil.ReadAll(r)
				require.NoError(t, err)
				assert.Equal(t, byteString, string(b))
			})

		for j := 0; j < iterCount; j++ {
			reader.EXPECT().Next().Return(true)
			reader.EXPECT().Current().Return(ts.Datapoint{
				Value:     float64(j),
				Timestamp: start,
			}, xtime.Second, nil)
			start = start.Add(time.Minute)
		}

		reader.EXPECT().Next().Return(false)
		reader.EXPECT().Err().Return(nil)
	}

	iter.Reset(records)
	i := 0
	for iter.Next() {
		dp, _, _ := iter.Current()
		// NB: iterator values should go [0,1,...,iterCount] for each block record.
		assert.Equal(t, float64(i%iterCount), dp.Value)
		// NB: time should be constantly increasing per value.
		assert.Equal(t, startTime.Add(time.Minute*time.Duration(i)), dp.Timestamp)
		i++
	}

	assert.Equal(t, count*iterCount, i)

	reader.EXPECT().Err().Return(errExpected)
	assert.Equal(t, errExpected, iter.Err())
	reader.EXPECT().Close()
	iter.Close()
}

func TestFailingCrossBlockIterator(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	reader := encoding.NewMockReaderIterator(ctrl)

	iterPool := encoding.NewMockReaderIteratorPool(ctrl)
	iterPool.EXPECT().Get().Return(reader)

	iter := NewCrossBlockIterator(iterPool)
	assert.False(t, iter.Next())

	count := 4
	iterCount := 5
	remaining := 12
	startTime := time.Now().Truncate(time.Hour)
	start := startTime
	records := make([]BlockRecord, 0, count)
	for i := 0; i < count; i++ {
		byteString := fmt.Sprint(i)
		data := []byte(byteString)

		if remaining == 0 {
			records = append(records, BlockRecord{
				Data: data,
			})
			continue
		}

		records = append(records, BlockRecord{
			Data: data,
		})

		reader.EXPECT().Reset(gomock.Any(), nil).Do(
			func(r io.Reader, _ namespace.SchemaDescr) {
				b, err := ioutil.ReadAll(r)
				require.NoError(t, err)
				assert.Equal(t, byteString, string(b))
			})

		for j := 0; remaining > 0 && j < iterCount; j++ {
			reader.EXPECT().Next().Return(true)
			reader.EXPECT().Current().Return(ts.Datapoint{
				Value:     float64(j),
				Timestamp: start,
			}, xtime.Second, nil)
			start = start.Add(time.Minute)
			remaining--
		}

		reader.EXPECT().Next().Return(false)
		if remaining == 0 {
			reader.EXPECT().Err().Return(errExpected).Times(2)
		} else {
			reader.EXPECT().Err().Return(nil)
		}
	}

	iter.Reset(records)
	i := 0
	for iter.Next() {
		dp, _, _ := iter.Current()
		// NB: iterator values should go [0,1,...,iterCount] for each block record.
		assert.Equal(t, float64(i%iterCount), dp.Value)
		// NB: time should be constantly increasing per value.
		assert.Equal(t, startTime.Add(time.Minute*time.Duration(i)), dp.Timestamp)
		i++
	}

	assert.Equal(t, 12, i)

	assert.Equal(t, errExpected, iter.Err())
	reader.EXPECT().Close()
	iter.Close()
}
