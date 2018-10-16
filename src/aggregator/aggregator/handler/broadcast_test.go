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

	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/metrics/metric/aggregated"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestBroadcastHandlerNewWriterSingleHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockWriter := writer.NewMockWriter(ctrl)
	handler := NewMockHandler(ctrl)
	handler.EXPECT().
		NewWriter(tally.NoopScope).
		Return(mockWriter, nil)
	h := NewBroadcastHandler([]Handler{handler})
	writer, err := h.NewWriter(tally.NoopScope)
	require.NoError(t, err)
	require.Equal(t, mockWriter, writer)
}

func TestBroadcastHandlerNewWriterMultiHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := aggregated.ChunkedMetricWithStoragePolicy{}

	var written int
	writer1 := writer.NewMockWriter(ctrl)
	writer1.EXPECT().
		Write(data).
		DoAndReturn(func(aggregated.ChunkedMetricWithStoragePolicy) error {
			written++
			return nil
		})
	writer2 := writer.NewMockWriter(ctrl)
	writer2.EXPECT().
		Write(data).
		DoAndReturn(func(aggregated.ChunkedMetricWithStoragePolicy) error {
			written += 2
			return nil
		})

	handler1 := NewMockHandler(ctrl)
	handler1.EXPECT().
		NewWriter(tally.NoopScope).
		Return(writer1, nil)
	handler2 := NewMockHandler(ctrl)
	handler2.EXPECT().
		NewWriter(tally.NoopScope).
		Return(writer2, nil)
	handlers := []Handler{handler1, handler2}

	h := NewBroadcastHandler(handlers)
	writer, err := h.NewWriter(tally.NoopScope)
	require.NoError(t, err)
	require.NoError(t, writer.Write(data))
	require.Equal(t, 3, written)
}

func TestBroadcastHandlerNewWriterMultiHandlerWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	handler1 := NewMockHandler(ctrl)
	handler1.EXPECT().
		NewWriter(tally.NoopScope).
		Return(writer.NewMockWriter(ctrl), nil)
	handler2 := NewMockHandler(ctrl)
	handler2.EXPECT().
		NewWriter(tally.NoopScope).
		Return(nil, errors.New("new writer error"))
	handlers := []Handler{handler1, handler2}

	h := NewBroadcastHandler(handlers)
	_, err := h.NewWriter(tally.NoopScope)
	require.Error(t, err)
}
