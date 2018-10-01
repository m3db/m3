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
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	xlog "github.com/m3db/m3x/log"
)

type bootstrapDataStep interface {
	bootstrapStep
	result() result.DataBootstrapResult
}

type bootstrapData struct {
	namespace    namespace.Metadata
	curr         bootstrap.Source
	next         bootstrap.Bootstrapper
	opts         bootstrap.RunOptions
	currResult   result.DataBootstrapResult
	nextResult   result.DataBootstrapResult
	mergedResult result.DataBootstrapResult
}

func newBootstrapDataStep(
	namespace namespace.Metadata,
	curr bootstrap.Source,
	next bootstrap.Bootstrapper,
	opts bootstrap.RunOptions,
) bootstrapDataStep {
	return &bootstrapData{
		namespace: namespace,
		curr:      curr,
		next:      next,
		opts:      opts,
	}
}

func (s *bootstrapData) prepare(
	totalRanges result.ShardTimeRanges,
) (bootstrapStepPreparedResult, error) {
	currAvailable, err := s.curr.AvailableData(s.namespace, totalRanges, s.opts)
	if err != nil {
		return bootstrapStepPreparedResult{}, err
	}

	return bootstrapStepPreparedResult{
		currAvailable: currAvailable,
	}, nil
}

func (s *bootstrapData) runCurrStep(
	targetRanges result.ShardTimeRanges,
) (bootstrapStepStatus, error) {
	var (
		requested = targetRanges.Copy()
		fulfilled result.ShardTimeRanges
		logFields []xlog.Field
		err       error
	)
	s.currResult, err = s.curr.ReadData(s.namespace, targetRanges, s.opts)
	if result := s.currResult; result != nil {
		fulfilled = requested
		fulfilled.Subtract(result.Unfulfilled())

		logFields = append(logFields,
			xlog.NewField("numSeries", result.ShardResults().NumSeries()))
	}
	return bootstrapStepStatus{
		fulfilled: fulfilled,
		logFields: logFields,
	}, err
}

func (s *bootstrapData) runNextStep(
	targetRanges result.ShardTimeRanges,
) (bootstrapStepStatus, error) {
	var (
		requested = targetRanges.Copy()
		fulfilled result.ShardTimeRanges
		err       error
	)
	s.nextResult, err = s.next.BootstrapData(s.namespace, targetRanges, s.opts)
	if result := s.nextResult; result != nil {
		fulfilled = requested
		fulfilled.Subtract(result.Unfulfilled())
	}
	return bootstrapStepStatus{
		fulfilled: fulfilled,
	}, err
}

func (s *bootstrapData) mergeResults(
	totalUnfulfilled result.ShardTimeRanges,
) {
	if s.mergedResult == nil {
		s.mergedResult = result.NewDataBootstrapResult()
	}
	if s.currResult != nil {
		// Merge the curr results in
		s.mergedResult.ShardResults().AddResults(s.currResult.ShardResults())
		s.currResult = nil
	}
	if s.nextResult != nil {
		// Merge the next results in
		s.mergedResult.ShardResults().AddResults(s.nextResult.ShardResults())
		s.nextResult = nil
	}
	s.mergedResult.SetUnfulfilled(totalUnfulfilled)
}

func (s *bootstrapData) result() result.DataBootstrapResult {
	return s.mergedResult
}
