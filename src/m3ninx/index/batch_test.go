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
	"testing"

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
