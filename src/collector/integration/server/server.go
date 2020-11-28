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

package server

import (
	"bufio"
	"io"
	"net"

	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	xserver "github.com/m3db/m3/src/x/server"

	"go.uber.org/zap"
)

// NewServer creates a new server.
func NewServer(addr string, opts Options) xserver.Server {
	handler := newHandler(opts.HandlerOptions())
	return xserver.NewServer(addr, handler, opts.TCPServerOptions())
}

type handler struct {
	logger         *zap.Logger
	itOpts         protobuf.UnaggregatedOptions
	readBufferSize int
	handleFn       HandleFn
}

func newHandler(opts HandlerOptions) *handler {
	return &handler{
		logger:         opts.InstrumentOptions().Logger(),
		itOpts:         opts.ProtobufUnaggregatedIteratorOptions(),
		readBufferSize: opts.ReadBufferSize(),
		handleFn:       opts.HandleFn(),
	}
}

func (h *handler) Handle(conn net.Conn) {
	reader := bufio.NewReaderSize(conn, h.readBufferSize)
	it := protobuf.NewUnaggregatedIterator(reader, h.itOpts)
	defer it.Close()

	for it.Next() {
		current := it.Current()
		var (
			metric    unaggregated.MetricUnion
			metadatas metadata.StagedMetadatas
		)
		switch current.Type {
		case encoding.CounterWithMetadatasType:
			metric = current.CounterWithMetadatas.Counter.ToUnion()
			metadatas = current.CounterWithMetadatas.StagedMetadatas
		case encoding.BatchTimerWithMetadatasType:
			metric = current.BatchTimerWithMetadatas.BatchTimer.ToUnion()
			metadatas = current.BatchTimerWithMetadatas.StagedMetadatas
		case encoding.GaugeWithMetadatasType:
			metric = current.GaugeWithMetadatas.Gauge.ToUnion()
			metadatas = current.GaugeWithMetadatas.StagedMetadatas
		default:
			h.logger.Error("unrecognized message type",
				zap.Any("messageType", current.Type),
			)
		}
		if err := h.handleFn(metric, metadatas); err != nil {
			h.logger.Error("error handling metrics",
				zap.Any("metric", metric),
				zap.Any("metadatas", metadatas),
				zap.Error(err),
			)
		}
	}

	if err := it.Err(); err != nil && err != io.EOF {
		h.logger.Error("decode error",
			zap.Error(err))
	}
}

func (h *handler) Close() {}
