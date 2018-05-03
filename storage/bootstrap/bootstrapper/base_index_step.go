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
	"time"

	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

type bootstrapIndexStep interface {
	bootstrapStep
	result() result.IndexBootstrapResult
}

type bootstrapIndex struct {
	namespace     namespace.Metadata
	totalRanges   xtime.Ranges
	curr          bootstrap.Source
	next          bootstrap.Bootstrapper
	currAvailable xtime.Ranges
	opts          bootstrap.RunOptions
	currResult    result.IndexBootstrapResult
	nextResult    result.IndexBootstrapResult
	lastResult    result.IndexBootstrapResult
	mergedResult  result.IndexBootstrapResult
}

func newBootstrapIndexStep(
	namespace namespace.Metadata,
	targetRanges xtime.Ranges,
	curr bootstrap.Source,
	next bootstrap.Bootstrapper,
	opts bootstrap.RunOptions,
) bootstrapIndexStep {
	available := curr.AvailableIndex(namespace, targetRanges)
	return &bootstrapIndex{
		namespace:     namespace,
		totalRanges:   targetRanges,
		curr:          curr,
		next:          next,
		currAvailable: available,
		opts:          opts,
	}
}

func (s *bootstrapIndex) currRanges() xtime.Ranges {
	return s.currAvailable
}

func (s *bootstrapIndex) nextRanges() xtime.Ranges {
	return s.totalRanges.RemoveRanges(s.currAvailable)
}

func (s *bootstrapIndex) prepare() bootstrapStepPreparedResult {
	var min, max time.Time
	iter := s.currAvailable.Iter()
	for iter.Next() {
		curr := iter.Value()
		if min.IsZero() || curr.Start.Before(min) {
			min = curr.Start
		}
		if max.IsZero() || curr.End.After(max) {
			max = curr.End
		}
	}
	return bootstrapStepPreparedResult{
		canFulfillAllWithCurr: s.nextRanges().IsEmpty(),
		logFields: []xlog.Field{
			xlog.NewField("from", min.String()),
			xlog.NewField("to", max.String()),
			xlog.NewField("range", max.Sub(min).String()),
		},
	}
}

func (s *bootstrapIndex) runCurrStep() (bootstrapStepStatus, error) {
	var (
		fulfilledAll bool
		logFields    []xlog.Field
		err          error
	)
	s.currResult, err = s.curr.ReadIndex(s.namespace, s.currRanges(), s.opts)
	if result := s.currResult; result != nil {
		fulfilledAll = !result.Unfulfilled().IsEmpty()
	}
	return bootstrapStepStatus{
		fulfilledAll: fulfilledAll,
		logFields:    logFields,
	}, err
}

func (s *bootstrapIndex) runNextStep() (bootstrapStepStatus, error) {
	var (
		fulfilledAll bool
		err          error
	)
	s.nextResult, err = s.next.BootstrapIndex(s.namespace, s.nextRanges(), s.opts)
	if result := s.nextResult; result != nil {
		fulfilledAll = !result.Unfulfilled().IsEmpty()
	}
	return bootstrapStepStatus{
		fulfilledAll: fulfilledAll,
	}, err
}

func (s *bootstrapIndex) mergeResults() bootstrapStepStatus {
	var (
		mergedResult    = result.NewIndexBootstrapResult()
		allRanges       = s.totalRanges
		fulfilledRanges = xtime.Ranges{}
	)
	if s.currResult != nil {
		// Merge the curr results in
		for _, block := range s.currResult.IndexResults() {
			mergedResult.Add(block, xtime.Ranges{})
		}

		// Calculate the fulfilled ranges for curr
		fulfilled := s.currRanges().RemoveRanges(s.currResult.Unfulfilled())
		fulfilledRanges = fulfilledRanges.AddRanges(fulfilled)
	}
	if s.nextResult != nil {
		// Merge the next results in
		for _, block := range s.nextResult.IndexResults() {
			mergedResult.Add(block, xtime.Ranges{})
		}

		// Calculate the fulfilled ranges for next
		fulfilled := s.nextRanges().RemoveRanges(s.nextResult.Unfulfilled())
		fulfilledRanges = fulfilledRanges.AddRanges(fulfilled)
	}
	unfulfilled := allRanges.RemoveRanges(fulfilledRanges)
	mergedResult.SetUnfulfilled(unfulfilled)
	s.mergedResult = mergedResult
	return bootstrapStepStatus{
		fulfilledAll: unfulfilled.IsEmpty(),
	}
}

func (s *bootstrapIndex) runLastStepAfterMergeResults() (bootstrapStepStatus, error) {
	unattemptedRanges := s.currResult.Unfulfilled()
	if s.nextResult == nil {
		// If we have never attempted the next time range then we want to also
		// attempt the ranges we didn't even try to attempt
		unattemptedRanges = unattemptedRanges.AddRanges(s.nextRanges())
	}

	var err error
	s.lastResult, err = s.next.BootstrapIndex(s.namespace, unattemptedRanges, s.opts)
	if result := s.lastResult; result != nil {
		// Union the results
		for _, block := range result.IndexResults() {
			s.mergedResult.Add(block, xtime.Ranges{})
		}

		// Work out what we fulfilled and then subtract that from current unfulfilled
		fulfilledRanges := unattemptedRanges.RemoveRanges(result.Unfulfilled())

		unfulfilled := s.mergedResult.Unfulfilled().RemoveRanges(fulfilledRanges)
		s.mergedResult.SetUnfulfilled(unfulfilled)
	}
	return bootstrapStepStatus{
		fulfilledAll: s.mergedResult.Unfulfilled().IsEmpty(),
	}, err
}

func (s *bootstrapIndex) result() result.IndexBootstrapResult {
	return s.mergedResult
}
