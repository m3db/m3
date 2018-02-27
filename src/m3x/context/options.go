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

package context

import "github.com/m3db/m3x/pool"

const (
	defaultInitFinalizersCap = 4
	defaultMaxFinalizersCap  = 1 << 16
)

type opts struct {
	contextPoolOpts        pool.ObjectPoolOptions
	finalizerPoolOpts      pool.ObjectPoolOptions
	maxPooledFinalizerCap  int
	initPooledFinalizerCap int
}

// NewOptions returns a new Options object.
func NewOptions() Options {
	return &opts{
		contextPoolOpts:        pool.NewObjectPoolOptions(),
		finalizerPoolOpts:      pool.NewObjectPoolOptions(),
		maxPooledFinalizerCap:  defaultMaxFinalizersCap,
		initPooledFinalizerCap: defaultInitFinalizersCap,
	}
}

func (o *opts) SetContextPoolOptions(po pool.ObjectPoolOptions) Options {
	opts := *o
	opts.contextPoolOpts = po
	return &opts
}

func (o *opts) ContextPoolOptions() pool.ObjectPoolOptions {
	return o.contextPoolOpts
}

func (o *opts) SetFinalizerPoolOptions(po pool.ObjectPoolOptions) Options {
	opts := *o
	opts.finalizerPoolOpts = po
	return &opts
}

func (o *opts) FinalizerPoolOptions() pool.ObjectPoolOptions {
	return o.finalizerPoolOpts
}

func (o *opts) SetMaxPooledFinalizerCapacity(max int) Options {
	opts := *o
	opts.maxPooledFinalizerCap = max
	return &opts
}

func (o *opts) MaxPooledFinalizerCapacity() int {
	return o.maxPooledFinalizerCap
}

func (o *opts) SetInitPooledFinalizerCapacity(init int) Options {
	opts := *o
	opts.initPooledFinalizerCap = init
	return &opts
}

func (o *opts) InitPooledFinalizerCapacity() int {
	return o.initPooledFinalizerCap
}
