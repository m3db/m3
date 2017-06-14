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
	"bytes"
	"testing"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func TestBroadcastHandler(t *testing.T) {
	var numCloses int
	mb := &mockBuffer{closeFn: func() { numCloses++ }}
	buf := aggregator.NewRefCountedBuffer(mb)
	bh := NewBroadcastHandler([]aggregator.Handler{NewBlackholeHandler()})
	require.NoError(t, bh.Handle(buf))
	require.Equal(t, 1, numCloses)
}

func TestBroadcastHandlerWithHandlerError(t *testing.T) {
	var numCloses int
	mb := &mockBuffer{
		buf:     bytes.NewBuffer(nil),
		closeFn: func() { numCloses++ },
	}
	_, err := mb.buf.Write([]byte{'a', 'b', 'c', 'd'})
	require.NoError(t, err)
	buf := aggregator.NewRefCountedBuffer(mb)

	bh := NewBroadcastHandler([]aggregator.Handler{
		NewLoggingHandler(instrument.NewOptions()),
		NewBlackholeHandler(),
	})
	require.Error(t, bh.Handle(buf))
	require.Equal(t, 1, numCloses)
}

func TestBroadcastHandlerWithNoHandlers(t *testing.T) {
	var numCloses int
	mb := &mockBuffer{
		buf:     bytes.NewBuffer(nil),
		closeFn: func() { numCloses++ },
	}
	buf := aggregator.NewRefCountedBuffer(mb)

	bh := NewBroadcastHandler(nil)
	require.NoError(t, bh.Handle(buf))
	require.Equal(t, 1, numCloses)
}

type closeFn func()

type mockBuffer struct {
	buf     *bytes.Buffer
	closeFn closeFn
}

func (mb *mockBuffer) Buffer() *bytes.Buffer { return mb.buf }
func (mb *mockBuffer) Reset()                {}
func (mb *mockBuffer) Bytes() []byte         { return mb.buf.Bytes() }
func (mb *mockBuffer) Close()                { mb.closeFn() }
