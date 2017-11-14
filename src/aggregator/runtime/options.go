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

package runtime

const (
	// A default rate limit value of 0 means rate limiting is disabled.
	defaultWriteValuesPerMetricLimitPerSecond = 0
)

// Options provide a set of options that are configurable at runtime.
type Options interface {
	// SetWriteValuesPerMetricLimitPerSecond sets the rate limit used
	// to cap the maximum number of values allowed to be written per second.
	SetWriteValuesPerMetricLimitPerSecond(value int64) Options

	// WriteValuesPerMetricLimitPerSecond returns the rate limit used
	// to cap the maximum number of values allowed to be written per second.
	WriteValuesPerMetricLimitPerSecond() int64
}

type options struct {
	writeValuesPerMetricLimitPerSecond int64
}

// NewOptions creates a new set of runtime options.
func NewOptions() Options {
	return &options{
		writeValuesPerMetricLimitPerSecond: defaultWriteValuesPerMetricLimitPerSecond,
	}
}

func (o *options) SetWriteValuesPerMetricLimitPerSecond(value int64) Options {
	opts := *o
	opts.writeValuesPerMetricLimitPerSecond = value
	return &opts
}

func (o *options) WriteValuesPerMetricLimitPerSecond() int64 {
	return o.writeValuesPerMetricLimitPerSecond
}
