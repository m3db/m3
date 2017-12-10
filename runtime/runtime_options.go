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

import (
	"time"

	"github.com/m3db/m3db/ratelimit"
)

const (
	defaultWriteNewSeriesAsync                  = false
	defaultWriteNewSeriesBackoffDuration        = time.Duration(0)
	defaultWriteNewSeriesLimitPerShardPerSecond = 0
	defaultTickPerSeriesSleepDuration           = 65 * time.Microsecond
)

type options struct {
	persistRateLimitOpts                 ratelimit.Options
	writeNewSeriesAsync                  bool
	writeNewSeriesBackoffDuration        time.Duration
	writeNewSeriesLimitPerShardPerSecond int
	tickPerSeriesSleepDuration           time.Duration
}

// NewOptions creates a new set of runtime options with defaults
func NewOptions() Options {
	return &options{
		persistRateLimitOpts:                 ratelimit.NewOptions(),
		writeNewSeriesAsync:                  defaultWriteNewSeriesAsync,
		writeNewSeriesBackoffDuration:        defaultWriteNewSeriesBackoffDuration,
		writeNewSeriesLimitPerShardPerSecond: defaultWriteNewSeriesLimitPerShardPerSecond,
		tickPerSeriesSleepDuration:           defaultTickPerSeriesSleepDuration,
	}
}

func (o *options) SetPersistRateLimitOptions(value ratelimit.Options) Options {
	opts := *o
	opts.persistRateLimitOpts = value
	return &opts
}

func (o *options) PersistRateLimitOptions() ratelimit.Options {
	return o.persistRateLimitOpts
}

func (o *options) SetWriteNewSeriesAsync(value bool) Options {
	opts := *o
	opts.writeNewSeriesAsync = value
	return &opts
}

func (o *options) WriteNewSeriesAsync() bool {
	return o.writeNewSeriesAsync
}

func (o *options) SetWriteNewSeriesBackoffDuration(value time.Duration) Options {
	opts := *o
	opts.writeNewSeriesBackoffDuration = value
	return &opts
}

func (o *options) WriteNewSeriesBackoffDuration() time.Duration {
	return o.writeNewSeriesBackoffDuration
}

func (o *options) SetWriteNewSeriesLimitPerShardPerSecond(value int) Options {
	opts := *o
	opts.writeNewSeriesLimitPerShardPerSecond = value
	return &opts
}

func (o *options) WriteNewSeriesLimitPerShardPerSecond() int {
	return o.writeNewSeriesLimitPerShardPerSecond
}

func (o *options) SetTickPerSeriesSleepDuration(value time.Duration) Options {
	opts := *o
	opts.tickPerSeriesSleepDuration = value
	return &opts
}

func (o *options) TickPerSeriesSleepDuration() time.Duration {
	return o.tickPerSeriesSleepDuration
}
