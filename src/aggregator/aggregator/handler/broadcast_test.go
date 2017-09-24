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

package handler

import (
	"errors"
	"testing"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/metric/aggregated"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestBroadcastHandlerNewWriterSingleHandler(t *testing.T) {
	mockWriter := &mockWriter{}
	handler := &mockHandler{
		newWriterFn: func(scope tally.Scope) (aggregator.Writer, error) {
			return mockWriter, nil
		},
	}
	h := NewBroadcastHandler([]aggregator.Handler{handler})
	writer, err := h.NewWriter(tally.NoopScope)
	require.NoError(t, err)
	require.Equal(t, mockWriter, writer)
}

func TestBroadcastHandlerNewWriterMultiHandler(t *testing.T) {
	var written int
	writers := []aggregator.Writer{
		&mockWriter{
			writeFn: func(aggregated.ChunkedMetricWithStoragePolicy) error {
				written++
				return nil
			},
		},
		&mockWriter{
			writeFn: func(aggregated.ChunkedMetricWithStoragePolicy) error {
				written += 2
				return nil
			},
		},
	}
	handlers := []aggregator.Handler{
		&mockHandler{
			newWriterFn: func(scope tally.Scope) (aggregator.Writer, error) {
				return writers[0], nil
			},
		},
		&mockHandler{
			newWriterFn: func(scope tally.Scope) (aggregator.Writer, error) {
				return writers[1], nil
			},
		},
	}
	h := NewBroadcastHandler(handlers)
	writer, err := h.NewWriter(tally.NoopScope)
	require.NoError(t, err)
	require.NoError(t, writer.Write(aggregated.ChunkedMetricWithStoragePolicy{}))
	require.Equal(t, 3, written)
}

func TestBroadcastHandlerNewWriterMultiHandlerWithError(t *testing.T) {
	handlers := []aggregator.Handler{
		&mockHandler{
			newWriterFn: func(scope tally.Scope) (aggregator.Writer, error) {
				return &mockWriter{}, nil
			},
		},
		&mockHandler{
			newWriterFn: func(scope tally.Scope) (aggregator.Writer, error) {
				return nil, errors.New("new writer error")
			},
		},
	}
	h := NewBroadcastHandler(handlers)
	_, err := h.NewWriter(tally.NoopScope)
	require.Error(t, err)
}

type writeFn func(mp aggregated.ChunkedMetricWithStoragePolicy) error
type writeflushFn func() error

type mockWriter struct {
	writeFn writeFn
	flushFn writeflushFn
}

func (w *mockWriter) Write(mp aggregated.ChunkedMetricWithStoragePolicy) error {
	return w.writeFn(mp)
}

func (w *mockWriter) Flush() error { return w.flushFn() }
func (w *mockWriter) Close() error { return nil }

type newWriterFn func(scope tally.Scope) (aggregator.Writer, error)

type mockHandler struct {
	newWriterFn newWriterFn
}

func (h *mockHandler) NewWriter(scope tally.Scope) (aggregator.Writer, error) {
	return h.newWriterFn(scope)
}

func (h *mockHandler) Close() {}
