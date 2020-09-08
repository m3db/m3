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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errExpected = errors.New("expected error")

func TestCrossBlockReaderRejectMisconfiguredInputs(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	dfsReader := NewMockDataFileSetReader(ctrl)
	dfsReader.EXPECT().OrderedByIndex().Return(false)

	_, err := NewCrossBlockReader([]DataFileSetReader{dfsReader}, instrument.NewTestOptions(t))

	assert.Equal(t, errReaderNotOrderedByIndex, err)
}

func TestCrossBlockReaderRejectMisorderedInputs(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now().Truncate(time.Hour)
	dfsReader1 := NewMockDataFileSetReader(ctrl)
	dfsReader1.EXPECT().OrderedByIndex().Return(true)
	dfsReader1.EXPECT().Range().Return(xtime.Range{Start: now})

	later := now.Add(time.Hour)
	dfsReader2 := NewMockDataFileSetReader(ctrl)
	dfsReader2.EXPECT().OrderedByIndex().Return(true)
	dfsReader2.EXPECT().Range().Return(xtime.Range{Start: later})

	_, err := NewCrossBlockReader([]DataFileSetReader{dfsReader2, dfsReader1}, instrument.NewTestOptions(t))

	expectedErr := fmt.Errorf("dataFileSetReaders are not ordered by time (%s followed by %s)", later, now)

	assert.Equal(t, expectedErr, err)
}

func TestCrossBlockReader(t *testing.T) {
	tests := []struct {
		name           string
		blockSeriesIDs [][]string
		expectedIDs    []string
	}{
		{
			name:           "no readers",
			blockSeriesIDs: [][]string{},
			expectedIDs:    []string{},
		},
		{
			name:           "empty readers",
			blockSeriesIDs: [][]string{{}, {}, {}},
			expectedIDs:    []string{},
		},
		{
			name:           "one reader, one series",
			blockSeriesIDs: [][]string{{"id1"}},
			expectedIDs:    []string{"id1"},
		},
		{
			name:           "one reader, many series",
			blockSeriesIDs: [][]string{{"id1", "id2", "id3"}},
			expectedIDs:    []string{"id1", "id2", "id3"},
		},
		{
			name:           "many readers with same series",
			blockSeriesIDs: [][]string{{"id1"}, {"id1"}, {"id1"}},
			expectedIDs:    []string{"id1"},
		},
		{
			name:           "many readers with different series",
			blockSeriesIDs: [][]string{{"id1"}, {"id2"}, {"id3"}},
			expectedIDs:    []string{"id1", "id2", "id3"},
		},
		{
			name:           "many readers with unordered series",
			blockSeriesIDs: [][]string{{"id3"}, {"id1"}, {"id2"}},
			expectedIDs:    []string{"id1", "id2", "id3"},
		},
		{
			name:           "complex case",
			blockSeriesIDs: [][]string{{"id2", "id3", "id5"}, {"id1", "id2", "id4"}, {"id1", "id4"}},
			expectedIDs:    []string{"id1", "id2", "id3", "id4", "id5"},
		},
		{
			name:           "duplicate ids within a block",
			blockSeriesIDs: [][]string{{"id1", "id2"}, {"id2", "id2"}},
			expectedIDs:    []string{"id1"},
		},
		{
			name:           "immediate reader error",
			blockSeriesIDs: [][]string{{"error"}},
			expectedIDs:    []string{},
		},
		{
			name:           "reader error later",
			blockSeriesIDs: [][]string{{"id1", "id2"}, {"id1", "error"}},
			expectedIDs:    []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testCrossBlockReader(t, tt.blockSeriesIDs, tt.expectedIDs)
		})
	}
}

func testCrossBlockReader(t *testing.T, blockSeriesIds [][]string, expectedIDs []string) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now().Truncate(time.Hour)
	var dfsReaders []DataFileSetReader

	expectedBlockCount := 0

	for blockIndex, ids := range blockSeriesIds {
		dfsReader := NewMockDataFileSetReader(ctrl)
		dfsReader.EXPECT().OrderedByIndex().Return(true)
		dfsReader.EXPECT().Range().Return(xtime.Range{Start: now.Add(time.Hour * time.Duration(blockIndex))}).AnyTimes()

		blockHasError := false
		for _, strID := range ids {
			if strID == "error" {
				dfsReader.EXPECT().Read().Return(nil, nil, nil, uint32(0), errExpected)
				blockHasError = true
			} else {
				id := ident.NewMockID(ctrl)
				gomock.InOrder(
					id.EXPECT().Bytes().Return([]byte(strID)).AnyTimes(),
					id.EXPECT().String().Return(strID).MaxTimes(1), // only called from test code
					id.EXPECT().Finalize(),
				)

				tags := ident.NewMockTagIterator(ctrl)
				tags.EXPECT().Close()

				data := checked.NewMockBytes(ctrl)
				gomock.InOrder(
					data.EXPECT().IncRef(),
					data.EXPECT().DecRef(),
					data.EXPECT().Finalize(),
				)

				checksum := uint32(blockIndex) // somewhat hacky - using checksum to propagate block index value for assertions

				dfsReader.EXPECT().Read().Return(id, tags, data, checksum, nil)
			}
		}

		if !blockHasError {
			dfsReader.EXPECT().Read().Return(nil, nil, nil, uint32(0), io.EOF).MaxTimes(1)
		}

		dfsReaders = append(dfsReaders, dfsReader)
		expectedBlockCount += len(ids)
	}

	cbReader, err := NewCrossBlockReader(dfsReaders, instrument.NewTestOptions(t))
	require.NoError(t, err)
	defer cbReader.Close()

	blockCount := 0
	seriesCount := 0
	for cbReader.Next() {
		id, tags, records := cbReader.Current()

		strID := id.String()
		id.Finalize()
		assert.Equal(t, expectedIDs[seriesCount], strID)

		assert.NotNil(t, tags)
		tags.Close()

		previousBlockIndex := -1
		for _, record := range records {
			blockIndex := int(record.DataChecksum) // see the comment above
			assert.True(t, blockIndex > previousBlockIndex, "same id blocks must be read in temporal order")

			previousBlockIndex = blockIndex
			assert.NotNil(t, record.Data)

			record.Data.Finalize()
		}

		blockCount += len(records)
		seriesCount++
	}

	assert.Equal(t, len(expectedIDs), seriesCount, "count of series read")

	err = cbReader.Err()
	if err == nil || (err.Error() != errExpected.Error() && !strings.HasPrefix(err.Error(), "duplicate id")) {
		require.NoError(t, cbReader.Err())
		assert.Equal(t, expectedBlockCount, blockCount, "count of blocks read")
	}

	for _, dfsReader := range dfsReaders {
		assert.NotNil(t, dfsReader)
	}
}

func TestSkippingReader(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now().Truncate(time.Hour)
	var dfsReaders []DataFileSetReader

	expectedBlockCount := 0

	blockSeriesIDs := [][]string{{"id1"}, {"id2"}, {"id3"}}
	expectedIDs := []string{"id3"}
	for blockIndex, ids := range blockSeriesIDs {
		dfsReader := NewMockDataFileSetReader(ctrl)
		dfsReader.EXPECT().OrderedByIndex().Return(true)
		dfsReader.EXPECT().Range().Return(xtime.Range{Start: now.Add(time.Hour * time.Duration(blockIndex))}).AnyTimes()

		blockHasError := false
		for j, id := range ids {
			tags := ident.NewTags(ident.StringTag("foo", strconv.Itoa(j)))
			data := checkedBytes([]byte{byte(j)})
			data.DecRef()                  // start with 0 ref
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
		expectedBlockCount += len(ids)
	}

	cbReader, err := NewCrossBlockReader(dfsReaders, instrument.NewTestOptions(t))
	require.NoError(t, err)
	defer cbReader.Close()

	blockCount := 0
	seriesCount := 0
	// NB: skip first two
	expectedBlockCount -= 2
	require.True(t, cbReader.Next())
	require.True(t, cbReader.Next())
	for cbReader.Next() {
		// NB: call Current twice to ensure it does not mutate values.
		id, _, _ := cbReader.Current()
		assert.Equal(t, expectedIDs[seriesCount], id.String())
		_, tags, records := cbReader.Current()

		strID := id.String()
		id.Finalize()
		assert.Equal(t, expectedIDs[seriesCount], strID)

		assert.NotNil(t, tags)
		tags.Close()

		previousBlockIndex := -1
		for _, record := range records {
			blockIndex := int(record.DataChecksum) // see the comment above
			assert.True(t, blockIndex > previousBlockIndex, "same id blocks must be read in temporal order")

			previousBlockIndex = blockIndex
			assert.NotNil(t, record.Data)
		}

		blockCount += len(records)
		seriesCount++
	}

	assert.Equal(t, len(expectedIDs), seriesCount, "count of series read")

	err = cbReader.Err()
	if err == nil || (err.Error() != errExpected.Error() && !strings.HasPrefix(err.Error(), "duplicate id")) {
		require.NoError(t, cbReader.Err())
		assert.Equal(t, expectedBlockCount, blockCount, "count of blocks read")
	}

	for _, dfsReader := range dfsReaders {
		assert.NotNil(t, dfsReader)
	}
}
