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
	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3x/errors"
)

type broadcastHandler struct {
	handlers []aggregator.Handler
}

// NewBroadcastHandler creates a new Handler that routes a
// msgpack buffer to a list of handlers.
func NewBroadcastHandler(handlers []aggregator.Handler) aggregator.Handler {
	return &broadcastHandler{handlers: handlers}
}

func (b *broadcastHandler) Handle(buffer *aggregator.RefCountedBuffer) error {
	multiErr := errors.NewMultiError()
	for _, h := range b.handlers {
		buffer.IncRef()
		if err := h.Handle(buffer); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	buffer.DecRef()
	return multiErr.FinalError()
}

func (b *broadcastHandler) Close() {
	for _, h := range b.handlers {
		h.Close()
	}
}
