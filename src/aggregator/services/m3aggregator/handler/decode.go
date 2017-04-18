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
	"io"

	"github.com/m3db/m3metrics/metric/aggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
)

// HandleFunc handles an aggregated metric alongside the policy.
type HandleFunc func(metric aggregated.Metric, policy policy.Policy) error

type decodingHandler struct {
	handle HandleFunc
}

// NewDecodingHandler creates a new decoding handler with a custom handle function.
func NewDecodingHandler(handle HandleFunc) Handler { return decodingHandler{handle: handle} }

func (h decodingHandler) Handle(buffer msgpack.Buffer) error {
	defer buffer.Close()

	iter := msgpack.NewAggregatedIterator(buffer.Buffer(), msgpack.NewAggregatedIteratorOptions())
	defer iter.Close()

	for iter.Next() {
		rawMetric, policy := iter.Value()
		metric, err := rawMetric.Metric()
		if err != nil {
			return err
		}
		if err := h.handle(metric, policy); err != nil {
			return err
		}
	}
	if err := iter.Err(); err != nil && err != io.EOF {
		return err
	}
	return nil
}
