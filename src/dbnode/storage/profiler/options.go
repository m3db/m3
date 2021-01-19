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

type options struct {
	bootstrapProfilerEnabled bool
	bootstrapProfilePath     string
}

// NewOptions creates new profiler options.
func NewOptions() Options {
	return &options{}
}

func (o *options) Enabled() bool {
	return o.bootstrapProfilerEnabled
}

func (o *options) SetEnabled(value bool) Options {
	opts := *o
	opts.bootstrapProfilerEnabled = value
	return &opts
}

func (o *options) ProfilePath() string {
	return o.bootstrapProfilePath
}

func (o *options) SetProfilePath(value string) Options {
	opts := *o
	opts.bootstrapProfilePath = value
	return &opts
}
