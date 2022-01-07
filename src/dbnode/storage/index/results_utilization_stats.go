// Copyright (c) 2022 Uber Technologies, Inc.
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

// resultsUtilizationStats tracks utilization of results object in order to detect and prevent
// unbounded growth of reusable object which may otherwise stay in the object pool indefinitely.
type resultsUtilizationStats struct {
	resultsMapCapacity            int
	consecutiveTimesUnderCapacity int
}

const (
	consecutiveTimesUnderCapacityThreshold = 10
)

// updateAndCheck returns true iff the underlying object is supposed to be returned
// to the appropriate object pool for reuse.
func (r *resultsUtilizationStats) updateAndCheck(totalDocsCount int) bool {
	if totalDocsCount < r.resultsMapCapacity {
		r.consecutiveTimesUnderCapacity++
	} else {
		r.resultsMapCapacity = totalDocsCount
		r.consecutiveTimesUnderCapacity = 0
	}

	// If the size of reusable result object is not fully utilized for a long enough time,
	// we want to drop it from the pool and prevent holding on to excessive memory indefinitely.
	return r.consecutiveTimesUnderCapacity < consecutiveTimesUnderCapacityThreshold
}
