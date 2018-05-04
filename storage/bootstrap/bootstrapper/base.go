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
	"github.com/m3db/m3db/storage/namespace"
	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
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
		bs = NewNoOpNoneBootstrapperProvider().Provide()
	}
	return baseBootstrapper{
		opts: opts,
		log:  opts.InstrumentOptions().Logger(),
		name: name,
		src:  src,
		next: bs,
	}
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
	step := newBootstrapDataStep(namespace, shardsTimeRanges,
		b.src, b.next, opts)
	err := b.runBootstrapStep(namespace, step)
	if err != nil {
		return nil, err
	}
	return step.result(), nil
}

func (b baseBootstrapper) BootstrapIndex(
	ns namespace.Metadata,
	timeRanges xtime.Ranges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	// FOLLOWUP(r): implement the parallelization part, maybe wrap up in
	// an interface the mergability logic, etc of both indexbootstrapresult
	// and databootstrapresult and use same code (poor mans generics solution)
	return result.NewIndexBootstrapResult(), nil
}

func (b baseBootstrapper) runBootstrapStep(
	namespace namespace.Metadata,
	step bootstrapStep,
) error {
	var (
		prepareResult    = step.prepare()
		wg               sync.WaitGroup
		currStatus       bootstrapStepStatus
		currErr, nextErr error
	)
	if !prepareResult.canFulfillAllWithCurr &&
		b.Can(bootstrap.BootstrapParallel) &&
		b.next.Can(bootstrap.BootstrapParallel) {
		// If ranges can be bootstrapped now from the next source then begin attempt now
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, nextErr = step.runNextStep()
		}()
	}

	logFields := append([]xlog.Field{
		xlog.NewField("source", b.name),
		xlog.NewField("namespace", namespace.ID().String()),
	}, prepareResult.logFields...)
	b.log.WithFields(logFields...).Infof("bootstrapping from source starting")

	nowFn := b.opts.ClockOptions().NowFn()
	begin := nowFn()

	currStatus, currErr = step.runCurrStep()

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

	currStatus = step.mergeResults()

	// If there are some time ranges the current bootstrapper could not fulfill,
	// pass it along to the next bootstrapper
	if !currStatus.fulfilledAll {
		_, lastErr := step.runLastStepAfterMergeResults()
		if lastErr != nil {
			return lastErr
		}
	}

	return nil
}
