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

package logical

import (
	"fmt"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"
)

// VectorMatchCardinality describes the cardinality relationship
// of two Vectors in a binary operation.
type VectorMatchCardinality int

const (
	// CardOneToOne is used for one-one relationship
	CardOneToOne VectorMatchCardinality = iota
	// CardManyToOne is used for many-one relationship
	CardManyToOne
	// CardOneToMany is used for one-many relationship
	CardOneToMany
	// CardManyToMany is used for many-many relationship
	CardManyToMany
)

// VectorMatching describes how elements from two Vectors in a binary
// operation are supposed to be matched.
type VectorMatching struct {
	// The cardinality of the two Vectors.
	Card VectorMatchCardinality
	// MatchingLabels contains the labels which define equality of a pair of
	// elements from the Vectors.
	MatchingLabels []string
	// On includes the given label names from matching,
	// rather than excluding them.
	On bool
	// Include contains additional labels that should be included in
	// the result from the side with the lower cardinality.
	Include []string
}

// hashFunc returns a function that calculates the signature for a metric
// ignoring the provided labels. If on, then the given labels are only used instead.
func hashFunc(on bool, names ...string) func(models.Tags) uint64 {
	if on {
		return func(tags models.Tags) uint64 { return tags.IDWithKeys(names...) }
	}
	return func(tags models.Tags) uint64 { return tags.IDWithExcludes(names...) }
}

const (
	// AndType uses values from left hand side for which there is a value in right hand side with exactly matching label sets.
	// Other elements are replaced by NaNs. The metric name and values are carried over from the left-hand side.
	AndType = "and"

	// OrType uses all values from left hand side, and appends values from the right hand side which do
	// not have corresponding tags on the right
	OrType = "or"

	// UnlessType uses all values from lhs which do not exist in rhs
	UnlessType = "unless"

	initIndexSliceLength = 10
)

var (
	errMismatchedBounds     = fmt.Errorf("block bounds are mismatched")
	errMismatchedStepCounts = fmt.Errorf("block step counts are mismatched")
	errConflictingTags      = fmt.Errorf("block tags conflict")
)

func combineMetadata(l, r block.Metadata) (block.Metadata, error) {
	if !l.Bounds.Equals(r.Bounds) {
		return block.Metadata{}, errMismatchedBounds
	}

	for k, v := range r.Tags {
		if lVal, ok := l.Tags[k]; ok {
			if lVal != v {
				return block.Metadata{}, errConflictingTags
			}
		}
		l.Tags[k] = v
	}

	return l, nil
}

func addValuesAtIndeces(idxArray []int, iter block.StepIter, builder block.Builder) error {
	index := 0
	for ; iter.Next(); index++ {
		step, err := iter.Current()
		if err != nil {
			return err
		}
		values := step.Values()
		for _, idx := range idxArray {
			builder.AppendValue(index, values[idx])
		}
	}
	return nil
}
