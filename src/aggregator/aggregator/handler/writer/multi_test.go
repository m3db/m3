// Copyright (c) 2017 Uber Technologies, Inc.
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

package writer

import (
	"errors"
	"testing"

	"github.com/m3db/m3/src/metrics/metric/aggregated"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMultiWriterWriteNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var written []aggregated.ChunkedMetricWithStoragePolicy
	writer := NewMockWriter(ctrl)
	writer.EXPECT().
		Write(testChunkedMetricWithStoragePolicy).
		Return(nil).
		Do(func(mp aggregated.ChunkedMetricWithStoragePolicy) {
			written = append(written, mp)
		}).
		Times(3)
	writers := []Writer{writer, writer, writer}
	multiWriter := NewMultiWriter(writers)
	require.NoError(t, multiWriter.Write(testChunkedMetricWithStoragePolicy))

	expected := []aggregated.ChunkedMetricWithStoragePolicy{
		testChunkedMetricWithStoragePolicy,
		testChunkedMetricWithStoragePolicy,
		testChunkedMetricWithStoragePolicy,
	}
	require.Equal(t, expected, written)
}

func TestMultiWriterWritePartialErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var written []aggregated.ChunkedMetricWithStoragePolicy
	writer1 := NewMockWriter(ctrl)
	writer1.EXPECT().
		Write(testChunkedMetricWithStoragePolicy).
		Return(nil).
		Do(func(mp aggregated.ChunkedMetricWithStoragePolicy) {
			written = append(written, mp)
		})
	writer2 := NewMockWriter(ctrl)
	writer2.EXPECT().
		Write(testChunkedMetricWithStoragePolicy).
		Return(errors.New("write error"))
	writers := []Writer{writer1, writer2}
	multiWriter := NewMultiWriter(writers)
	require.Error(t, multiWriter.Write(testChunkedMetricWithStoragePolicy))

	expected := []aggregated.ChunkedMetricWithStoragePolicy{
		testChunkedMetricWithStoragePolicy,
	}
	require.Equal(t, expected, written)
}

func TestMultiWriterFlushNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushed int
	writer := NewMockWriter(ctrl)
	writer.EXPECT().
		Flush().
		Return(nil).
		Do(func() {
			flushed++
		}).
		Times(3)
	writers := []Writer{writer, writer, writer}
	multiWriter := NewMultiWriter(writers)
	require.NoError(t, multiWriter.Flush())
	require.Equal(t, 3, flushed)
}

func TestMultiWriterFlushPartialErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var flushSuccess int
	writer1 := NewMockWriter(ctrl)
	writer1.EXPECT().
		Flush().
		Return(nil).
		Do(func() {
			flushSuccess++
		})
	writer2 := NewMockWriter(ctrl)
	writer2.EXPECT().
		Flush().
		Return(errors.New("flush error"))

	writers := []Writer{writer1, writer2}
	multiWriter := NewMultiWriter(writers)
	require.Error(t, multiWriter.Flush())
	require.Equal(t, 1, flushSuccess)
}
