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
	"testing"
	"time"

	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var expectedError = errors.New("expected error")

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
		blockSeriesIds [][]string
	}{
		{"no readers", [][]string{}},
		{"empty readers", [][]string{{}, {}, {}}},
		{"one reader, one series", [][]string{{"id1"}}},
		{"one reader, many series", [][]string{{"id1", "id2", "id3"}}},
		{"many readers with same series", [][]string{{"id1"}, {"id1"}, {"id1"}}},
		{"many readers with different series", [][]string{{"id1"}, {"id2"}, {"id3"}}},
		{"many readers with unordered series", [][]string{{"id3"}, {"id1"}, {"id2"}}},
		{"complex case", [][]string{{"id2", "id3", "id5"}, {"id1", "id2", "id4"}, {"id1", "id4"}}},
		{"reader error", [][]string{{"id1", "id2"}, {"id1", "error"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCrossBlockReader(t, tt.blockSeriesIds)
		})
	}
}

func testCrossBlockReader(t *testing.T, blockSeriesIds [][]string) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now().Truncate(time.Hour)
	var dfsReaders []DataFileSetReader
	expectedCount := 0

	for blockIndex, ids := range blockSeriesIds {
		dfsReader := NewMockDataFileSetReader(ctrl)
		dfsReader.EXPECT().IsOrderedByIndex().Return(true)
		dfsReader.EXPECT().Range().Return(xtime.Range{Start: now.Add(time.Hour * time.Duration(blockIndex))})

		blockHasError := false
		for j, id := range ids {
			tags := ident.NewTags(ident.StringTag("foo", string(j)))
			data := checkedBytes([]byte{byte(j)})
			checksum := uint32(blockIndex) // somewhat hacky - using checksum to propagate block index value for assertions
			if id == "error" {
				dfsReader.EXPECT().Read().Return(nil, nil, nil, uint32(0), expectedError)
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

	hadError := false
	actualCount := 0
	previousId := ""
	var previousBlockIndex uint32
	for {
		id, tags, data, checksum, err := cbReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil && err.Error() == expectedError.Error() {
			hadError = true
			break
		}
		require.NoError(t, err)

		strId := id.String()
		id.Finalize()
		blockIndex := checksum // see the comment above
		assert.True(t, strId >= previousId, "series must be read in non-decreasing id order")
		if strId == previousId {
			assert.True(t, blockIndex >= previousBlockIndex, "same id blocks must be read in temporal order")
		}

		assert.NotNil(t, tags)
		tags.Close()

		assert.NotNil(t, data)
		data.DecRef()
		data.Finalize()

		previousId = strId
		previousBlockIndex = blockIndex

		actualCount++
	}

	if !hadError {
		assert.Equal(t, expectedCount, actualCount, "count of series read")
	}
}
