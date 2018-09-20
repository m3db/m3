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

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"
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
) (bootstrap.Bootstrapper, error) {
	var (
		bs  = next
		err error
	)
	if next == nil {
		bs, err = NewNoOpNoneBootstrapperProvider().Provide()
		if err != nil {
			return nil, err
		}
	}
	return baseBootstrapper{
		opts: opts,
		log:  opts.InstrumentOptions().Logger(),
		name: name,
		src:  src,
		next: bs,
	}, nil
}

// String returns the name of the bootstrapper.
func (b baseBootstrapper) String() string {
	return baseBootstrapperName
}

func (b baseBootstrapper) Can(strategy bootstrap.Strategy) bool {
	return b.src.Can(strategy)
}

func (b baseBootstrapper) BootstrapData(
	namespace namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}
	step := newBootstrapDataStep(namespace, b.src, b.next, opts)
	err := b.runBootstrapStep(namespace, shardsTimeRanges, step)
	if err != nil {
		return nil, err
	}
	return step.result(), nil
}

func (b baseBootstrapper) BootstrapIndex(
	namespace namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return result.NewIndexBootstrapResult(), nil
	}
	step := newBootstrapIndexStep(namespace, b.src, b.next, opts)
	err := b.runBootstrapStep(namespace, shardsTimeRanges, step)
	if err != nil {
		return nil, err
	}
	return step.result(), nil
}

func (b baseBootstrapper) runBootstrapStep(
	namespace namespace.Metadata,
	totalRanges result.ShardTimeRanges,
	step bootstrapStep,
) error {
	prepareResult, err := step.prepare(totalRanges)
	if err != nil {
		return err
	}

	var (
		wg                     sync.WaitGroup
		currStatus, nextStatus bootstrapStepStatus
		currErr, nextErr       error
		nextAttempted          bool
	)
	currRanges := prepareResult.currAvailable
	nextRanges := totalRanges.Copy()
	nextRanges.Subtract(currRanges)
	if !nextRanges.IsEmpty() &&
		b.Can(bootstrap.BootstrapParallel) &&
		b.next.Can(bootstrap.BootstrapParallel) {
		// If ranges can be bootstrapped now from the next source then begin attempt now
		nextAttempted = true
		wg.Add(1)
		go func() {
			defer wg.Done()
			nextStatus, nextErr = step.runNextStep(nextRanges)
		}()
	}

	min, max := currRanges.MinMax()
	logFields := []xlog.Field{
		xlog.NewField("source", b.name),
		xlog.NewField("namespace", namespace.ID().String()),
		xlog.NewField("from", min.String()),
		xlog.NewField("to", max.String()),
		xlog.NewField("range", max.Sub(min).String()),
		xlog.NewField("shards", len(currRanges)),
	}
	b.log.WithFields(logFields...).Infof("bootstrapping from source starting")

	nowFn := b.opts.ClockOptions().NowFn()
	begin := nowFn()

	currStatus, currErr = step.runCurrStep(currRanges)

	logFields = append(logFields, xlog.NewField("took", nowFn().Sub(begin).String()))
	if currErr != nil {
		logFields = append(logFields, xlog.NewField("error", currErr.Error()))
		b.log.WithFields(logFields...).Infof("bootstrapping from source completed with error")
	} else {
		logFields = append(logFields, currStatus.logFields...)
		b.log.WithFields(logFields...).Infof("bootstrapping from source completed successfully")
	}

	wg.Wait()
	if err := xerrors.FirstError(currErr, nextErr); err != nil {
		return err
	}

	fulfilledRanges := result.ShardTimeRanges{}
	fulfilledRanges.AddRanges(currStatus.fulfilled)
	fulfilledRanges.AddRanges(nextStatus.fulfilled)
	unfulfilled := totalRanges.Copy()
	unfulfilled.Subtract(fulfilledRanges)

	step.mergeResults(unfulfilled)

	unattemptedNextRanges := currRanges.Copy()
	unattemptedNextRanges.Subtract(currStatus.fulfilled)
	if !nextAttempted {
		// If we have never attempted the next time range then we want to also
		// attempt the ranges we didn't even try to attempt
		unattemptedNextRanges.AddRanges(nextRanges)
	}

	// If there are some time ranges the current bootstrapper could not fulfill,
	// that we can attempt then pass it along to the next bootstrapper.
	// NB(r): We explicitly do not ask the next bootstrapper to retry ranges
	// it could not fulfill as it's unlikely to be able to now.
	if !unattemptedNextRanges.IsEmpty() {
		nextStatus, nextErr = step.runNextStep(unattemptedNextRanges)
		if nextErr != nil {
			return nextErr
		}

		unfulfilledFinal := unfulfilled.Copy()
		unfulfilledFinal.Subtract(nextStatus.fulfilled)
		step.mergeResults(unfulfilledFinal)
	}

	return nil
}
