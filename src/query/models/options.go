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

package models

import (
	"errors"
)

var (
	defaultMetricName = []byte("__name__")

	errNoName = errors.New("metric name is missing or empty")
)

type tagOptions struct {
	metricName []byte
}

// NewTagOptions builds a new tag options with default values.
func NewTagOptions() TagOptions {
	return &tagOptions{
		metricName: defaultMetricName,
	}
}

func (o *tagOptions) Validate() error {
	if o.MetricName() == nil {
		return errNoName
	}

	return nil
}

func (o *tagOptions) SetMetricName(metricName []byte) TagOptions {
	opts := *o
	opts.metricName = metricName
	return &opts
}

func (o *tagOptions) MetricName() []byte {
	return o.metricName
}
