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

package tsdb

import (
	"time"

	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3x/time"
)

// FetchRange is a fetch range.
type FetchRange struct {
	xtime.Range
	policy.StoragePolicy
}

// Equal returns whether two FetchRanges are equal.
func (r FetchRange) Equal(other FetchRange) bool {
	return r.Range.Equal(other.Range) && r.StoragePolicy == other.StoragePolicy
}

// FetchRanges is a list of fetch ranges.
type FetchRanges []FetchRange

// FetchRequest is a request to fetch data from a source for a given id.
type FetchRequest struct {
	ID     string
	Ranges FetchRanges
}

// NewSingleRangeRequest creates a new single-range request.
func NewSingleRangeRequest(id string, start, end time.Time, p policy.StoragePolicy) FetchRequest {
	rng := xtime.Range{Start: start, End: end}
	ranges := []FetchRange{{Range: rng, StoragePolicy: p}}
	return FetchRequest{ID: id, Ranges: ranges}
}
