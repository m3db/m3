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

package xretry

import "time"

const (
	defaultInitialBackoff = time.Second
	defaultBackoffFactor  = 2.0
	defaultMax            = 2
	defaultJitter         = true
)

type options struct {
	initialBackoff time.Duration
	backoffFactor  float64
	max            int
	jitter         bool
}

// NewOptions creates new retry options
func NewOptions() Options {
	return &options{
		initialBackoff: defaultInitialBackoff,
		backoffFactor:  defaultBackoffFactor,
		max:            defaultMax,
		jitter:         defaultJitter,
	}
}

func (o *options) InitialBackoff(value time.Duration) Options {
	opts := *o
	opts.initialBackoff = value
	return &opts
}

func (o *options) GetInitialBackoff() time.Duration {
	return o.initialBackoff
}

func (o *options) BackoffFactor(value float64) Options {
	opts := *o
	opts.backoffFactor = value
	return &opts
}

func (o *options) GetBackoffFactor() float64 {
	return o.backoffFactor
}

func (o *options) Max(value int) Options {
	opts := *o
	opts.max = value
	return &opts
}

func (o *options) GetMax() int {
	return o.max
}

func (o *options) Jitter(value bool) Options {
	opts := *o
	opts.jitter = value
	return &opts
}

func (o *options) GetJitter() bool {
	return o.jitter
}
