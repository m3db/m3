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

package bootstrapper

import (
	"sync"

	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
)

const (
	baseBootstrapperName = "base"
)

// baseBootstrapper provides a skeleton for the interface methods.
type baseBootstrapper struct {
	opts result.Options
	log  xlog.Logger
	name string
	src  bootstrap.Source
	next bootstrap.Bootstrapper
}

// NewBaseBootstrapper creates a new base bootstrapper.
func NewBaseBootstrapper(
	name string,
	src bootstrap.Source,
	opts result.Options,
	next bootstrap.Bootstrapper,
) bootstrap.Bootstrapper {
	bs := next
	if next == nil {
		bs = NewNoOpNoneBootstrapper()
	}
	return &baseBootstrapper{
		opts: opts,
		log:  opts.InstrumentOptions().Logger(),
		name: name,
		src:  src,
		next: bs,
	}
}

func (b *baseBootstrapper) Can(strategy bootstrap.Strategy) bool {
	return b.src.Can(strategy)
}

func (b *baseBootstrapper) Bootstrap(
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	available := b.src.Available(namespace, shardsTimeRanges)
	remaining := shardsTimeRanges.Copy()
	remaining.Subtract(available)

	var (
		wg                     sync.WaitGroup
		currResult, nextResult result.BootstrapResult
		currErr, nextErr       error
	)
	if !remaining.IsEmpty() &&
		b.Can(bootstrap.BootstrapParallel) &&
		b.next.Can(bootstrap.BootstrapParallel) {
		// If ranges can be bootstrapped now from the next source then begin attempt now
		wg.Add(1)
		go func() {
			defer wg.Done()
			nextResult, nextErr = b.next.Bootstrap(namespace, remaining, opts)
		}()
	}

	min, max := available.MinMax()
	logFields := []xlog.LogField{
		xlog.NewLogField("source", b.name),
		xlog.NewLogField("from", min),
		xlog.NewLogField("to", max),
		xlog.NewLogField("range", max.Sub(min).String()),
		xlog.NewLogField("shards", len(available)),
	}
	b.log.WithFields(logFields...).Infof("bootstrapping from source starting")

	nowFn := b.opts.ClockOptions().NowFn()
	begin := nowFn()

	currResult, currErr = b.src.Read(namespace, available, opts)

	logFields = append(logFields, xlog.NewLogField("took", nowFn().Sub(begin).String()))
	if currErr != nil {
		logFields = append(logFields, xlog.NewLogField("error", currErr.Error()))
		b.log.WithFields(logFields...).Infof("bootstrapping from source completed with error")
	} else {
		b.log.WithFields(logFields...).Infof("bootstrapping from source completed successfully")
	}

	wg.Wait()
	if err := xerrors.FirstError(currErr, nextErr); err != nil {
		return nil, err
	}

	if currResult == nil {
		currResult = result.NewBootstrapResult()
	}

	var (
		mergedResult         = currResult
		currUnfulfilled      = currResult.Unfulfilled()
		firstNextUnfulfilled result.ShardTimeRanges
	)
	if nextResult != nil {
		// Union the results
		mergedResult.ShardResults().AddResults(nextResult.ShardResults())
		// Save the first next unfulfilled time ranges
		firstNextUnfulfilled = nextResult.Unfulfilled()
	} else {
		// Union just the unfulfilled ranges from current and the remaining ranges
		currUnfulfilled.AddRanges(remaining)
	}

	// If there are some time ranges the current bootstrapper could not fulfill,
	// pass it along to the next bootstrapper
	if !currUnfulfilled.IsEmpty() {
		nextResult, nextErr = b.next.Bootstrap(namespace, currUnfulfilled, opts)
		if nextErr != nil {
			return nil, nextErr
		}

		if nextResult != nil {
			// Union the results
			mergedResult.ShardResults().AddResults(nextResult.ShardResults())

			// Set the unfulfilled ranges and don't use a union considering the
			// next bootstrapper was asked to fulfill all outstanding ranges of
			// the current bootstrapper
			mergedResult.SetUnfulfilled(nextResult.Unfulfilled())
		}
	}

	if nextResult != nil {
		// Make sure to add any unfulfilled time ranges from the
		// first time the next bootstrapper was asked to execute if it was
		// executed in parallel
		mergedResult.Unfulfilled().AddRanges(firstNextUnfulfilled)
	}

	return mergedResult, nil
}

// String returns the name of the bootstrapper.
func (b *baseBootstrapper) String() string {
	return baseBootstrapperName
}
