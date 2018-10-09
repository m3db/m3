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

import "time"

const (
	// A default rate limit value of 0 means rate limiting is disabled.
	defaultWriteValuesPerMetricLimitPerSecond = 0

	// A default rate limit value of 0 means rate limiting is disabled.
	defaultWriteNewMetricLimitPerShardPerSecond = 0

	// A default warmup duration of 0 means there is no warmup.
	defaultWriteNewMetricNoLimitWarmupDuration = 0
)

// Options provide a set of options that are configurable at runtime.
type Options interface {
	// SetWriteValuesPerMetricLimitPerSecond sets the rate limit used
	// to cap the maximum number of values allowed to be written per second.
	SetWriteValuesPerMetricLimitPerSecond(value int64) Options

	// WriteValuesPerMetricLimitPerSecond returns the rate limit used
	// to cap the maximum number of values allowed to be written per second.
	WriteValuesPerMetricLimitPerSecond() int64

	// SetWriteNewMetricLimitPerShardPerSecond sets the rate limit used
	// to cap the maximum number of new metrics allowed to be written per shard per second.
	SetWriteNewMetricLimitPerShardPerSecond(value int64) Options

	// WriteNewMetricLimitPerShardPerSecond returns the rate limit used
	// to cap the maximum number of new metrics allowed to be written per shard per second.
	WriteNewMetricLimitPerShardPerSecond() int64

	// SetWriteNewMetricNoLimitWarmupDuration sets the warmup duration during which
	// rate limiting is not applied for writing new metric series for the purpose
	// of not aggresively limiting new series insertion rate when an instance just
	// comes online and is ingesting a large number of new series during startup.
	// The warmup duration is in effect starting from the time when the first entry
	// is insert into the shard.
	SetWriteNewMetricNoLimitWarmupDuration(value time.Duration) Options

	// WriteNewMetricNoLimitWarmupDuration returns the warmup duration during which
	// rate limiting is not applied for writing new metric series for the purpose
	// of not aggresively limiting new series insertion rate when an instance just
	// comes online and is ingesting a large number of new series during startup.
	// The warmup duration is in effect starting from the time when the first entry
	// is insert into the shard.
	WriteNewMetricNoLimitWarmupDuration() time.Duration
}

type options struct {
	writeValuesPerMetricLimitPerSecond   int64
	writeNewMetricLimitPerShardPerSecond int64
	writeNewMetricNoLimitWarmupDuration  time.Duration
}

// NewOptions creates a new set of runtime options.
func NewOptions() Options {
	return &options{
		writeValuesPerMetricLimitPerSecond:   defaultWriteValuesPerMetricLimitPerSecond,
		writeNewMetricLimitPerShardPerSecond: defaultWriteNewMetricLimitPerShardPerSecond,
		writeNewMetricNoLimitWarmupDuration:  defaultWriteNewMetricNoLimitWarmupDuration,
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

func (o *options) SetWriteNewMetricLimitPerShardPerSecond(value int64) Options {
	opts := *o
	opts.writeNewMetricLimitPerShardPerSecond = value
	return &opts
}

func (o *options) WriteNewMetricLimitPerShardPerSecond() int64 {
	return o.writeNewMetricLimitPerShardPerSecond
}

func (o *options) SetWriteNewMetricNoLimitWarmupDuration(value time.Duration) Options {
	opts := *o
	opts.writeNewMetricNoLimitWarmupDuration = value
	return &opts
}

func (o *options) WriteNewMetricNoLimitWarmupDuration() time.Duration {
	return o.writeNewMetricNoLimitWarmupDuration
}
