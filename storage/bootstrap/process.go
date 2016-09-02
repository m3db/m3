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

	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

var (
	errUnfulfilledRanges = errors.New("bootstrap finished with unfulfilled ranges")
)

// bootstrapProcess represents the bootstrapping process.
type bootstrapProcess struct {
	opts         Options
	log          xlog.Logger
	bootstrapper Bootstrapper
}

// NewBootstrapProcess creates a new bootstrap process.
func NewBootstrapProcess(
	opts Options,
	bootstrapper Bootstrapper,
) Bootstrap {
	return &bootstrapProcess{
		opts:         opts,
		log:          opts.GetInstrumentOptions().GetLogger(),
		bootstrapper: bootstrapper,
	}
}

// targetRanges calculates the the target time ranges.
// NB(xichen): bootstrapping is now a two-step process: we bootstrap the data between
// [writeStart - retentionPeriod, writeStart - bufferPast) in the first step, and the
// data between [writeStart - bufferPast, writeStart + bufferFuture) in the second step.
func (b *bootstrapProcess) targetRanges(writeStart time.Time) xtime.Ranges {
	start := writeStart.Add(-b.opts.GetRetentionOptions().GetRetentionPeriod())
	midPoint := writeStart.Add(-b.opts.GetRetentionOptions().GetBufferPast())
	end := writeStart.Add(b.opts.GetRetentionOptions().GetBufferFuture())

	return xtime.NewRanges().
		AddRange(xtime.Range{Start: start, End: midPoint}).
		AddRange(xtime.Range{Start: midPoint, End: end})
}

// Run initiates the bootstrap process, where writeStart is when we start
// accepting incoming writes.
func (b *bootstrapProcess) Run(
	writeStart time.Time,
	namespace string,
	shards []uint32,
) (Result, error) {
	// Initialize the target ranges we'd like to bootstrap
	targetRanges := b.targetRanges(writeStart)

	result := NewResult()
	it := targetRanges.Iter()
	for it.Next() {
		shardsTimeRanges := make(ShardTimeRanges, len(shards))

		window := it.Value()

		r := xtime.NewRanges().AddRange(window)
		for _, s := range shards {
			shardsTimeRanges[s] = r
		}

		b.log.WithFields(
			xlog.NewLogField("shardsLen", len(shards)),
			xlog.NewLogField("from", window.Start.String()),
			xlog.NewLogField("to", window.End.String()),
			xlog.NewLogField("length", window.End.Sub(window.Start).String()),
		).Infof("bootstrapping shards for range")

		res, err := b.bootstrapper.Bootstrap(namespace, shardsTimeRanges)
		if err != nil {
			return nil, err
		}
		result.AddResult(res)
	}

	return result, nil
}
