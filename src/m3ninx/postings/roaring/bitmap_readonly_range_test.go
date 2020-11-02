// Copyright (c) 2020 Uber Technologies, Inc.
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

package roaring

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/stretchr/testify/require"
)

func TestReadonlyRangePostingsListContainerIterator(t *testing.T) {
	type testDef struct {
		startLeadingEmptyContainers int
		startAtBoundary             bool
		endLeadingEmptyContainers   int
		endAtBoundary               bool
	}

	var tests []testDef
	for _, startAtBoundary := range []bool{false, true} {
		for _, endAtBoundary := range []bool{false, true} {
			emptyContainers := []int{0, 1, 3}
			for _, startLeadingEmptyContainers := range emptyContainers {
				for _, endLeadingEmptyContainers := range emptyContainers {
					tests = append(tests, testDef{
						startLeadingEmptyContainers: startLeadingEmptyContainers,
						startAtBoundary:             startAtBoundary,
						endLeadingEmptyContainers:   endLeadingEmptyContainers,
						endAtBoundary:               endAtBoundary,
					})
				}
			}
		}
	}

	rng := rand.New(rand.NewSource(seed))
	for i, test := range tests {
		t.Run(fmt.Sprintf("i=%d, test=+%v", i, test), func(t *testing.T) {
			start := uint64(test.startLeadingEmptyContainers) * containerValues
			if !test.startAtBoundary {
				start += uint64(rng.Int63n(int64(containerValues)))
			}

			end := (uint64(test.startLeadingEmptyContainers) * containerValues) +
				(uint64(test.endLeadingEmptyContainers) * containerValues)
			if !test.endAtBoundary {
				// Calculate random number with the remaining range we have.
				endMin := end
				if start > endMin {
					endMin = start
				}
				endMax := end + containerValues
				endRange := endMax - endMin
				// Calculate new end as min + range.
				end = endMin + uint64(rng.Int63n(int64(endRange)))
			} else {
				// Add an extra container's worth of values to interpret
				end += containerValues
			}

			pl, err := NewReadOnlyRangePostingsList(start, end)
			require.NoError(t, err)

			expected := postings.NewRangeIterator(postings.ID(start), postings.ID(end))
			require.True(t, postings.EqualIterator(expected, pl.Iterator()))
		})
	}
}
