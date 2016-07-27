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

package bootstrap

import (
	"errors"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3x/time"
)

const (
	// defaultThrottlePeriod is how long we wait till the next iteration of bootstrap starts by default.
	defaultThrottlePeriod = 10 * time.Second

	// defaultMaxRetries is the maximum number of bootstrap retries by default.
	defaultMaxRetries = 3
)

var (
	errMaxRetriesExceeded = errors.New("max retries exceeded")
)

// Options represents the options for bootstrapping.
type Options interface {

	// ThrottlePeriod sets how long we wait till the next iteration of bootstrap starts by default.
	ThrottlePeriod(value time.Duration) Options

	// GetThrottlePeriod returns how long we wait till the next iteration of bootstrap starts by default.
	GetThrottlePeriod() time.Duration

	// MaxRetries is the maximum number of bootstrap retries.
	MaxRetries(value int) Options

	// GetMaxRetries returns the maximum number of bootstrap retries.
	GetMaxRetries() int
}

type options struct {
	throttlePeriod time.Duration
	maxRetries     int
}

// NewOptions creates new bootstrap options.
func NewOptions() Options {
	return &options{
		throttlePeriod: defaultThrottlePeriod,
		maxRetries:     defaultMaxRetries,
	}
}

// ThrottlePeriod sets how long we wait till the next iteration of bootstrap starts by default.
func (o *options) ThrottlePeriod(value time.Duration) Options {
	opts := *o
	opts.throttlePeriod = value
	return &opts
}

// GetThrottlePeriod returns how long we wait till the next iteration of bootstrap starts by default.
func (o *options) GetThrottlePeriod() time.Duration {
	return o.throttlePeriod
}

// MaxRetries sets the maximum number of bootstrap retries.
func (o *options) MaxRetries(value int) Options {
	opts := *o
	opts.maxRetries = value
	return &opts
}

// GetMaxRetries returns the maximum number of bootstrap retries.
func (o *options) GetMaxRetries() int {
	return o.maxRetries
}

// bootstrapProcess represents the bootstrapping process.
type bootstrapProcess struct {
	bsOpts       Options
	dbOpts       m3db.DatabaseOptions
	bootstrapper m3db.Bootstrapper
}

// NewBootstrapProcess creates a new bootstrap process.
func NewBootstrapProcess(
	bsOpts Options,
	dbOpts m3db.DatabaseOptions,
	bootstrapper m3db.Bootstrapper,
) m3db.Bootstrap {
	if bsOpts == nil {
		bsOpts = NewOptions()
	}
	if dbOpts == nil {
		dbOpts = storage.NewDatabaseOptions()
	}
	return &bootstrapProcess{
		bsOpts:       bsOpts,
		dbOpts:       dbOpts,
		bootstrapper: bootstrapper,
	}
}

// initTimeRanges initializes the target time ranges.
// NB(xichen): bootstrapping is now a two-step process: we bootstrap the data between
// [writeStart - retentionPeriod, writeStart - bufferPast) in the first step, and the
// data between [writeStart - bufferPast, writeStart + bufferFuture) in the second step.
// The reason why this is necessary is because writers can write data in the past or in
// the future, and as a result, future writes between [writeStart - bufferFuture, writeStart)
// are lost, which means the data buffered between [writeStart, writeStart + bufferFuture)
// cannot be trusted and therefore need to be bootstrapped as well, which means in the worst
// case we need to wait till writeStart + bufferPast + bufferFuture before bootstrap can complete.
func (b *bootstrapProcess) initTimeRanges(writeStart time.Time) xtime.Ranges {
	start := writeStart.Add(-b.dbOpts.GetRetentionPeriod())
	midPoint := writeStart.Add(-b.dbOpts.GetBufferPast())
	end := writeStart.Add(b.dbOpts.GetBufferFuture())

	return xtime.NewRanges().
		AddRange(xtime.Range{Start: start, End: midPoint}).
		AddRange(xtime.Range{Start: midPoint, End: end})
}

// bootstrap returns the bootstrapped results for a given shard time ranges.
func (b *bootstrapProcess) bootstrap(shard uint32, timeRanges xtime.Ranges) (m3db.ShardResult, error) {
	results := NewShardResult(b.dbOpts)
	for i := 0; i < b.bsOpts.GetMaxRetries(); i++ {
		res, unfulfilled := b.bootstrapper.Bootstrap(shard, timeRanges)
		results.AddResult(res)
		if xtime.IsEmpty(unfulfilled) {
			return results, nil
		}
		// If there are still unfulfilled time ranges, sleep for a while and retry.
		time.Sleep(b.bsOpts.GetThrottlePeriod())
	}
	return results, errMaxRetriesExceeded
}

// Run initiates the bootstrap process, where writeStart is when we start
// accepting incoming writes.
func (b *bootstrapProcess) Run(writeStart time.Time, shard uint32) (m3db.ShardResult, error) {

	// initializes the target range we'd like to bootstrap
	unfulfilledRanges := b.initTimeRanges(writeStart)

	return b.bootstrap(shard, unfulfilledRanges)
}
