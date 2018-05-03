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

package bootstrapper

import (
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	xlog "github.com/m3db/m3x/log"
)

type bootstrapDataStep interface {
	bootstrapStep
	result() result.DataBootstrapResult
}

type bootstrapData struct {
	namespace     namespace.Metadata
	totalRanges   result.ShardTimeRanges
	curr          bootstrap.Source
	next          bootstrap.Bootstrapper
	currAvailable result.ShardTimeRanges
	opts          bootstrap.RunOptions
	currResult    result.DataBootstrapResult
	nextResult    result.DataBootstrapResult
	lastResult    result.DataBootstrapResult
	mergedResult  result.DataBootstrapResult
}

func newBootstrapDataStep(
	namespace namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	curr bootstrap.Source,
	next bootstrap.Bootstrapper,
	opts bootstrap.RunOptions,
) bootstrapDataStep {
	available := curr.AvailableData(namespace, shardsTimeRanges)
	return &bootstrapData{
		namespace:     namespace,
		totalRanges:   shardsTimeRanges,
		curr:          curr,
		next:          next,
		currAvailable: available,
		opts:          opts,
	}
}

func (s *bootstrapData) currRanges() result.ShardTimeRanges {
	return s.currAvailable
}

func (s *bootstrapData) nextRanges() result.ShardTimeRanges {
	remaining := s.totalRanges.Copy()
	remaining.Subtract(s.currAvailable)
	return remaining
}

func (s *bootstrapData) prepare() bootstrapStepPreparedResult {
	min, max := s.currAvailable.MinMax()
	return bootstrapStepPreparedResult{
		canFulfillAllWithCurr: s.nextRanges().IsEmpty(),
		logFields: []xlog.Field{
			xlog.NewField("from", min.String()),
			xlog.NewField("to", max.String()),
			xlog.NewField("range", max.Sub(min).String()),
			xlog.NewField("shards", len(s.currAvailable)),
		},
	}
}

func (s *bootstrapData) runCurrStep() (bootstrapStepStatus, error) {
	var (
		fulfilledAll bool
		logFields    []xlog.Field
		err          error
	)
	s.currResult, err = s.curr.ReadData(s.namespace, s.currRanges(), s.opts)
	if result := s.currResult; result != nil {
		fulfilledAll = !result.Unfulfilled().IsEmpty()

		logField := xlog.NewField("numSeries", result.ShardResults().NumSeries())
		logFields = append(logFields, logField)
	}
	return bootstrapStepStatus{
		fulfilledAll: fulfilledAll,
		logFields:    logFields,
	}, err
}

func (s *bootstrapData) runNextStep() (bootstrapStepStatus, error) {
	var (
		fulfilledAll bool
		err          error
	)
	s.nextResult, err = s.next.BootstrapData(s.namespace, s.nextRanges(), s.opts)
	if result := s.nextResult; result != nil {
		fulfilledAll = !result.Unfulfilled().IsEmpty()
	}
	return bootstrapStepStatus{
		fulfilledAll: fulfilledAll,
	}, err
}

func (s *bootstrapData) mergeResults() bootstrapStepStatus {
	var (
		mergedResult    = result.NewDataBootstrapResult()
		allRanges       = s.totalRanges.Copy()
		fulfilledRanges = result.ShardTimeRanges{}
	)
	if s.currResult != nil {
		// Merge the curr results in
		mergedResult.ShardResults().AddResults(s.currResult.ShardResults())

		// Calculate the fulfilled ranges for curr
		fulfilled := s.currRanges().Copy()
		fulfilled.Subtract(s.currResult.Unfulfilled().Copy())
		fulfilledRanges.AddRanges(fulfilled)
	}
	if s.nextResult != nil {
		// Merge the next results in
		mergedResult.ShardResults().AddResults(s.nextResult.ShardResults())

		// Calculate the fulfilled ranges for next
		fulfilled := s.nextRanges().Copy()
		fulfilled.Subtract(s.nextResult.Unfulfilled().Copy())
		fulfilledRanges.AddRanges(fulfilled)
	}
	unfulfilled := allRanges.Copy()
	unfulfilled.Subtract(fulfilledRanges)
	mergedResult.SetUnfulfilled(unfulfilled)
	s.mergedResult = mergedResult
	return bootstrapStepStatus{
		fulfilledAll: unfulfilled.IsEmpty(),
	}
}

func (s *bootstrapData) runLastStepAfterMergeResults() (bootstrapStepStatus, error) {
	unattemptedRanges := s.currResult.Unfulfilled().Copy()
	if s.nextResult == nil {
		// If we have never attempted the next time range then we want to also
		// attempt the ranges we didn't even try to attempt
		unattemptedRanges.AddRanges(s.nextRanges())
	}

	var err error
	s.lastResult, err = s.next.BootstrapData(s.namespace, unattemptedRanges, s.opts)
	if result := s.lastResult; result != nil {
		// Union the results
		s.mergedResult.ShardResults().AddResults(result.ShardResults())

		// Work out what we fulfilled and then subtract that from current unfulfilled
		fulfilledRanges := unattemptedRanges.Copy()
		fulfilledRanges.Subtract(result.Unfulfilled())

		unfulfilled := s.mergedResult.Unfulfilled().Copy()
		unfulfilled.Subtract(fulfilledRanges)
		s.mergedResult.SetUnfulfilled(unfulfilled)
	}
	return bootstrapStepStatus{
		fulfilledAll: s.mergedResult.Unfulfilled().IsEmpty(),
	}, err
}

func (s *bootstrapData) result() result.DataBootstrapResult {
	return s.mergedResult
}
