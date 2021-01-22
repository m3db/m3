// Copyright (c) 2021 Uber Technologies, Inc.
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

package profiler

import "fmt"

type options struct {
	enabled  bool
	profiler Profiler
}

// NewOptions creates new profiler options.
func NewOptions() Options {
	return &options{}
}

func (o *options) Validate() error {
	if o.enabled && o.profiler == nil {
		return fmt.Errorf("profiler is enabled but is not set")
	}
	return nil
}

func (o *options) Enabled() bool {
	return o.enabled
}

func (o *options) SetEnabled(value bool) Options {
	opts := *o
	opts.enabled = value
	return &opts
}

func (o *options) Profiler() Profiler {
	return o.profiler
}

func (o *options) SetProfiler(value Profiler) Options {
	opts := *o
	opts.profiler = value
	return &opts
}
