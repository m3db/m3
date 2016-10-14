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

	"github.com/m3db/m3db/ts"
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
		log:          opts.InstrumentOptions().Logger(),
		bootstrapper: bootstrapper,
	}
}

func (b *bootstrapProcess) Run(
	targetRanges xtime.Ranges,
	namespace ts.ID,
	shards []uint32,
) (Result, error) {
	result := NewResult()
	it := targetRanges.Iter()
	for it.Next() {
		shardsTimeRanges := make(ShardTimeRanges, len(shards))

		window := it.Value()

		r := xtime.NewRanges().AddRange(window)
		for _, s := range shards {
			shardsTimeRanges[s] = r
		}

		logFields := []xlog.LogField{
			xlog.NewLogField("bootstrapper", b.bootstrapper.String()),
			xlog.NewLogField("namespace", namespace.String()),
			xlog.NewLogField("numShards", len(shards)),
			xlog.NewLogField("from", window.Start.String()),
			xlog.NewLogField("to", window.End.String()),
			xlog.NewLogField("range", window.End.Sub(window.Start).String()),
		}
		b.log.WithFields(logFields...).Infof("bootstrapping shards for range starting")

		nowFn := b.opts.ClockOptions().NowFn()
		begin := nowFn()

		res, err := b.bootstrapper.Bootstrap(namespace, shardsTimeRanges)

		logFields = append(logFields, xlog.NewLogField("took", nowFn().Sub(begin).String()))
		if err != nil {
			logFields = append(logFields, xlog.NewLogField("error", err.Error()))
			b.log.WithFields(logFields...).Infof("bootstrapping shards for range completed with error")
			return nil, err
		}

		b.log.WithFields(logFields...).Infof("bootstrapping shards for range completed successfully")
		result.AddResult(res)
	}

	return result, nil
}
