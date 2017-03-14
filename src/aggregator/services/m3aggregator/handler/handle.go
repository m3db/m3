// Copyright (c) 2016 Uber Technologies, Inc.
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
	"github.com/m3db/m3x/log"
)

var log = xlog.NewLevelLogger(xlog.SimpleLogger, xlog.LogLevelInfo)

// Handler handles encoded streams containing aggregated metrics alongside their policies
type Handler interface {
	// Handle processes aggregated metrics and policies encoded in the buffer
	Handle(buffer msgpack.Buffer) error
}

// HandleFunc handles an aggregated metric alongside the policy
type HandleFunc func(metric aggregated.Metric, policy policy.Policy) error

func defaultHandle(metric aggregated.Metric, policy policy.Policy) error {
	log.WithFields(
		xlog.NewLogField("metric", metric.String()),
		xlog.NewLogField("policy", policy.String()),
	).Info("aggregated metric")
	return nil
}

type handler struct {
	handle HandleFunc
}

// NewDefaultHandler creates the default handler
func NewDefaultHandler() Handler { return NewHandler(defaultHandle) }

// NewHandler creates a new handler with a custom handle function
func NewHandler(handle HandleFunc) Handler { return handler{handle: handle} }

func (h handler) Handle(buffer msgpack.Buffer) error {
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
