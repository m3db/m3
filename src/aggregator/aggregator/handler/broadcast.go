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
	"github.com/m3db/m3aggregator/aggregator/handler/writer"

	"github.com/uber-go/tally"
)

type broadcastHandler struct {
	handlers []aggregator.Handler
}

// NewBroadcastHandler creates a new Handler that broadcasts incoming data
// to a list of handlers.
func NewBroadcastHandler(handlers []aggregator.Handler) aggregator.Handler {
	return &broadcastHandler{handlers: handlers}
}

func (h *broadcastHandler) NewWriter(scope tally.Scope) (aggregator.Writer, error) {
	if len(h.handlers) == 1 {
		return h.handlers[0].NewWriter(scope)
	}
	writers := make([]aggregator.Writer, 0, len(h.handlers))
	for _, handler := range h.handlers {
		writer, err := handler.NewWriter(scope)
		if err != nil {
			return nil, err
		}
		writers = append(writers, writer)
	}
	return writer.NewMultiWriter(writers), nil
}

func (h *broadcastHandler) Close() {
	for _, handler := range h.handlers {
		handler.Close()
	}
}
