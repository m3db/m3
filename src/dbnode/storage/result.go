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

package storage

type tickResult struct {
	activeSeries           int
	expiredSeries          int
	activeBlocks           int
	wiredBlocks            int
	unwiredBlocks          int
	pendingMergeBlocks     int
	madeExpiredBlocks      int
	madeUnwiredBlocks      int
	mergedOutOfOrderBlocks int
	errors                 int
	evictedBuckets         int
}

func (r tickResult) merge(other tickResult) tickResult {
	return tickResult{
		activeSeries:           r.activeSeries + other.activeSeries,
		expiredSeries:          r.expiredSeries + other.expiredSeries,
		activeBlocks:           r.activeBlocks + other.activeBlocks,
		wiredBlocks:            r.wiredBlocks + other.wiredBlocks,
		pendingMergeBlocks:     r.pendingMergeBlocks + other.pendingMergeBlocks,
		unwiredBlocks:          r.unwiredBlocks + other.unwiredBlocks,
		madeExpiredBlocks:      r.madeExpiredBlocks + other.madeExpiredBlocks,
		madeUnwiredBlocks:      r.madeUnwiredBlocks + other.madeUnwiredBlocks,
		mergedOutOfOrderBlocks: r.mergedOutOfOrderBlocks + other.mergedOutOfOrderBlocks,
		errors:                 r.errors + other.errors,
		evictedBuckets:         r.evictedBuckets + other.evictedBuckets,
	}
}
