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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errExpected = errors.New("expected error")

func TestCrossBlockReaderRejectMisconfiguredInputs(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dfsReader := NewMockDataFileSetReader(ctrl)
	dfsReader.EXPECT().IsOrderedByIndex().Return(false)

	_, err := NewCrossBlockReader([]DataFileSetReader{dfsReader})

	assert.Equal(t, errReaderNotOrderedByIndex, err)
}

func TestCrossBlockReaderRejectMisorderedInputs(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now().Truncate(time.Hour)
	dfsReader1 := NewMockDataFileSetReader(ctrl)
	dfsReader1.EXPECT().IsOrderedByIndex().Return(true)
	dfsReader1.EXPECT().Range().Return(xtime.Range{Start: now})

	later := now.Add(time.Hour)
	dfsReader2 := NewMockDataFileSetReader(ctrl)
	dfsReader2.EXPECT().IsOrderedByIndex().Return(true)
	dfsReader2.EXPECT().Range().Return(xtime.Range{Start: later})

	_, err := NewCrossBlockReader([]DataFileSetReader{dfsReader2, dfsReader1})

	expectedErr := fmt.Errorf("dataFileSetReaders are not ordered by time (%s followed by %s)", later, now)

	assert.Equal(t, expectedErr, err)
}

func TestCrossBlockReader(t *testing.T) {
	tests := []struct {
		name           string
		blockSeriesIDs [][]string
	}{
		{"no readers", [][]string{}},
		{"empty readers", [][]string{{}, {}, {}}},
		{"one reader, one series", [][]string{{"id1"}}},
		{"one reader, many series", [][]string{{"id1", "id2", "id3"}}},
		{"many readers with same series", [][]string{{"id1"}, {"id1"}, {"id1"}}},
		{"many readers with different series", [][]string{{"id1"}, {"id2"}, {"id3"}}},
		{"many readers with unordered series", [][]string{{"id3"}, {"id1"}, {"id2"}}},
		{"complex case", [][]string{{"id2", "id3", "id5"}, {"id1", "id2", "id4"}, {"id1", "id4"}}},
		{"immediate reader error", [][]string{{"error"}}},
		{"reader error later", [][]string{{"id1", "id2"}, {"id1", "error"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCrossBlockReader(t, tt.blockSeriesIDs)
		})
	}
}

func testCrossBlockReader(t *testing.T, blockSeriesIDs [][]string) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now().Truncate(time.Hour)
	var dfsReaders []DataFileSetReader
	expectedCount := 0

	for blockIndex, ids := range blockSeriesIDs {
		dfsReader := NewMockDataFileSetReader(ctrl)
		dfsReader.EXPECT().IsOrderedByIndex().Return(true)
		dfsReader.EXPECT().Range().Return(xtime.Range{Start: now.Add(time.Hour * time.Duration(blockIndex))})

		blockHasError := false
		for j, id := range ids {
			tags := ident.NewTags(ident.StringTag("foo", string(j)))
			data := checkedBytes([]byte{byte(j)})
			checksum := uint32(blockIndex) // somewhat hacky - using checksum to propagate block index value for assertions
			if id == "error" {
				dfsReader.EXPECT().Read().Return(nil, nil, nil, uint32(0), errExpected)
				blockHasError = true
			} else {
				dfsReader.EXPECT().Read().Return(ident.StringID(id), ident.NewTagsIterator(tags), data, checksum, nil)
			}
		}

		if !blockHasError {
			dfsReader.EXPECT().Read().Return(nil, nil, nil, uint32(0), io.EOF).MaxTimes(1)
		}

		dfsReaders = append(dfsReaders, dfsReader)
		expectedCount += len(ids)
	}

	cbReader, err := NewCrossBlockReader(dfsReaders)
	require.NoError(t, err)
	defer cbReader.Close()

	actualCount := 0
	previousID := ""
	for cbReader.Next() {
		id, tags, records := cbReader.Current()

		strID := id.String()
		id.Finalize()
		assert.True(t, strID > previousID, "series must be read in increasing id order")

		assert.NotNil(t, tags)
		tags.Close()

		var previousBlockIndex uint32
		for _, record := range records {
			blockIndex := record.Checksum // see the comment above
			assert.True(t, blockIndex >= previousBlockIndex, "same id blocks must be read in temporal order")
			previousBlockIndex = blockIndex
			assert.NotNil(t, record.Data)
			record.Data.DecRef()
			record.Data.Finalize()
		}

		previousID = strID
		actualCount += len(records)
	}

	err = cbReader.Err()
	if err == nil || err.Error() != errExpected.Error() {
		require.NoError(t, cbReader.Err())
		assert.Equal(t, expectedCount, actualCount, "count of series read")
	}

	for _, dfsReader := range dfsReaders {
		assert.NotNil(t, dfsReader)
	}
}

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

		data := checked.NewMockBytes(ctrl)
		gomock.InOrder(
			data.EXPECT().IncRef(),
			data.EXPECT().Bytes().Return([]byte(byteString)),
			data.EXPECT().DecRef(),
			data.EXPECT().Finalize(),
		)
		records = append(records, BlockRecord{
			Data: data,
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

	count := 3
	iterCount := 5
	remaining := 12
	startTime := time.Now().Truncate(time.Hour)
	start := startTime
	records := make([]BlockRecord, 0, count)
	for i := 0; i < count; i++ {
		byteString := fmt.Sprint(i)

		data := checked.NewMockBytes(ctrl)

		if remaining == 0 {
			// NB: ensure all data is cleaned on an error.
			gomock.InOrder(
				data.EXPECT().DecRef(),
				data.EXPECT().Finalize(),
			)

			records = append(records, BlockRecord{
				Data: data,
			})

			continue
		}

		gomock.InOrder(
			data.EXPECT().IncRef(),
			data.EXPECT().Bytes().Return([]byte(byteString)),
			data.EXPECT().DecRef(),
			data.EXPECT().Finalize(),
		)
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

	// reader.EXPECT().Err().Return(errExpected)
	assert.Equal(t, errExpected, iter.Err())
	reader.EXPECT().Close()
	iter.Close()
}
