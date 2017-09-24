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

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metric/aggregated"

	"github.com/stretchr/testify/require"
)

func TestMultiWriterWriteNoError(t *testing.T) {
	var written []aggregated.ChunkedMetricWithStoragePolicy
	writer := &mockWriter{
		writeFn: func(mp aggregated.ChunkedMetricWithStoragePolicy) error {
			written = append(written, mp)
			return nil
		},
	}
	writers := []aggregator.Writer{writer, writer, writer}
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
	var written []aggregated.ChunkedMetricWithStoragePolicy
	writer1 := &mockWriter{
		writeFn: func(mp aggregated.ChunkedMetricWithStoragePolicy) error {
			written = append(written, mp)
			return nil
		},
	}
	writer2 := &mockWriter{
		writeFn: func(mp aggregated.ChunkedMetricWithStoragePolicy) error {
			return errors.New("write error")
		},
	}
	writers := []aggregator.Writer{writer1, writer2}
	multiWriter := NewMultiWriter(writers)
	require.Error(t, multiWriter.Write(testChunkedMetricWithStoragePolicy))

	expected := []aggregated.ChunkedMetricWithStoragePolicy{
		testChunkedMetricWithStoragePolicy,
	}
	require.Equal(t, expected, written)
}

func TestMultiWriterFlushNoError(t *testing.T) {
	var flushed int
	writer := &mockWriter{
		flushFn: func() error { flushed++; return nil },
	}
	writers := []aggregator.Writer{writer, writer, writer}
	multiWriter := NewMultiWriter(writers)
	require.NoError(t, multiWriter.Flush())
	require.Equal(t, 3, flushed)
}

func TestMultiWriterFlushPartialErrors(t *testing.T) {
	var flushSuccess int
	writer1 := &mockWriter{
		flushFn: func() error { flushSuccess++; return nil },
	}
	writer2 := &mockWriter{
		flushFn: func() error { return errors.New("flush error") },
	}
	writers := []aggregator.Writer{writer1, writer2}
	multiWriter := NewMultiWriter(writers)
	require.Error(t, multiWriter.Flush())
	require.Equal(t, 1, flushSuccess)
}

type writeFn func(mp aggregated.ChunkedMetricWithStoragePolicy) error
type flushFn func() error

type mockWriter struct {
	writeFn writeFn
	flushFn flushFn
}

func (w *mockWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	return w.writeFn(mp)
}

func (w *mockWriter) Flush() error {
	return w.flushFn()
}

func (w *mockWriter) Close() error { return nil }
