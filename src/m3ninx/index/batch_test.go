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

package index

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestBatchAllowPartialUpdates(t *testing.T) {
	tests := []struct {
		name     string
		batch    Batch
		expected bool
	}{
		{
			name:     "off",
			batch:    NewBatch(nil),
			expected: false,
		},
		{
			name:     "on",
			batch:    NewBatch(nil, AllowPartialUpdates()),
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, test.batch.AllowPartialUpdates)
		})
	}
}

func TestBatchPartialError(t *testing.T) {
	var (
		idxs = []int{3, 7, 13}
		err  = NewBatchPartialError()
	)
	require.True(t, err.IsEmpty())

	for _, idx := range idxs {
		err.Add(BatchError{errors.New("error"), idx})
	}
	require.False(t, err.IsEmpty())

	var actualIdxs []int
	for _, err := range err.Errs() {
		actualIdxs = append(actualIdxs, err.Idx)
	}
	require.Equal(t, idxs, actualIdxs)

	require.True(t, IsBatchPartialError(err))
	require.False(t, IsBatchPartialError(errors.New("error")))
}

func TestBatchPartialErrorDupeFilter(t *testing.T) {
	var (
		idxs = []int{3, 7, 13}
		err  = NewBatchPartialError()
	)
	require.True(t, err.IsEmpty())

	for _, idx := range idxs {
		err.Add(BatchError{ErrDuplicateID, idx})
	}
	require.False(t, err.IsEmpty())

	var actualIdxs []int
	for _, err := range err.Errs() {
		actualIdxs = append(actualIdxs, err.Idx)
	}

	filtered := err.FilterDuplicateIDErrors()
	require.Nil(t, filtered)
}

func TestBatchPartialErrorDupeFilterIncludeOther(t *testing.T) {
	testErr := fmt.Errorf("testerr")

	err := NewBatchPartialError()
	err.Add(BatchError{ErrDuplicateID, 0})
	filtered := err.FilterDuplicateIDErrors()
	require.Nil(t, filtered)

	err.Add(BatchError{testErr, 1})
	filtered = err.FilterDuplicateIDErrors()
	require.NotNil(t, filtered)

}

func TestBatchPartialErrorDupeFilterSingleElement(t *testing.T) {
	testErr := fmt.Errorf("testerr")

	err := NewBatchPartialError()
	err.Add(BatchError{testErr, 1})
	original := err.FilterDuplicateIDErrors()
	require.NotNil(t, original)

	err.Add(BatchError{ErrDuplicateID, 0})
	after := err.FilterDuplicateIDErrors()
	require.Equal(t, original.Error(), after.Error())
}

func TestBatchPartialErrorFilterPropTest(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 1000
	parameters.MaxSize = 40
	parameters.Rng = rand.New(rand.NewSource(seed))
	parameters.Workers = 1
	properties := gopter.NewProperties(parameters)

	properties.Property("Filtering errors works", prop.ForAll(
		func(errStrings []string) bool {
			errs := make([]error, 0, len(errStrings))
			for _, err := range errStrings {
				errs = append(errs, errors.New(err))
			}
			numDupeErrors := 0
			b := NewBatchPartialError()
			for i := range errs {
				if errs[i] == ErrDuplicateID {
					numDupeErrors++
				}
				b.Add(BatchError{errs[i], i})
			}
			nb := b.FilterDuplicateIDErrors()
			if nb == nil {
				return numDupeErrors == len(errs)
			}
			return len(nb.errs) == len(errs)-numDupeErrors
		},
		gen.SliceOf(genError()),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func genError() gopter.Gen {
	return gen.OneConstOf(ErrDocNotFound.Error(), ErrDuplicateID.Error())
}
