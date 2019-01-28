// Copyright (c) 2018 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/aggregator/aggregator/handler/writer"
	"github.com/m3db/m3/src/msg/producer"

	"github.com/uber-go/tally"
)

type protobufHandler struct {
	p    producer.Producer
	opts writer.Options
}

// NewProtobufHandler creates a new protobuf handler.
func NewProtobufHandler(p producer.Producer, opts writer.Options) Handler {
	return protobufHandler{
		p:    p,
		opts: opts,
	}
}

func (h protobufHandler) NewWriter(scope tally.Scope) (writer.Writer, error) {
	iOpts := h.opts.InstrumentOptions()
	return writer.NewProtobufWriter(
		h.p, h.opts.SetInstrumentOptions(iOpts.SetMetricsScope(scope)),
	), nil
}

func (h protobufHandler) Close() {
	h.p.Close(producer.WaitForConsumption)
}
