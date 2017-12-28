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

package msgpack

import (
	"io"
	"net"

	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"
	xserver "github.com/m3db/m3x/server"
)

// NewServer creates a new msgpack server.
func NewServer(addr string, opts Options) xserver.Server {
	handler := newHandler(opts.HandleFn(), opts.InstrumentOptions().Logger())
	return xserver.NewServer(addr, handler, opts.ServerOptions())
}

type handler struct {
	handleFn HandleFn
	logger   log.Logger
}

func newHandler(handleFn HandleFn, logger log.Logger) *handler {
	return &handler{
		handleFn: handleFn,
		logger:   logger,
	}
}

func (h *handler) Handle(conn net.Conn) {
	it := msgpack.NewUnaggregatedIterator(conn, nil)
	defer it.Close()

	for it.Next() {
		metric := it.Metric()
		policiesList := it.PoliciesList()
		if err := h.handleFn(metric, policiesList); err != nil {
			h.logger.WithFields(
				log.NewField("metric", metric),
				log.NewField("policiesList", policiesList),
				log.NewErrField(err),
			).Error("error handling metrics")
		}
	}

	if err := it.Err(); err != nil && err != io.EOF {
		h.logger.WithFields(
			log.NewErrField(err),
		).Error("decode error")
	}
}

func (h *handler) Close() {}
