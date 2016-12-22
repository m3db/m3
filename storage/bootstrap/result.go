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

package bootstrap

import (
	"github.com/m3db/m3db/client/result"
	"github.com/m3db/m3x/time"
)

type bootstrapResult struct {
	results     result.ShardResults
	unfulfilled result.ShardTimeRanges
}

// NewResult creates a new result.
func NewResult() Result {
	return &bootstrapResult{
		results:     make(result.ShardResults),
		unfulfilled: make(result.ShardTimeRanges),
	}
}

func (r *bootstrapResult) ShardResults() result.ShardResults {
	return r.results
}

func (r *bootstrapResult) Unfulfilled() result.ShardTimeRanges {
	return r.unfulfilled
}

func (r *bootstrapResult) Add(shard uint32, shardResult result.ShardResult, unfulfilled xtime.Ranges) {
	r.results.AddResults(result.ShardResults{shard: shardResult})
	r.unfulfilled.AddRanges(result.ShardTimeRanges{shard: unfulfilled})
}

func (r *bootstrapResult) SetUnfulfilled(unfulfilled result.ShardTimeRanges) {
	r.unfulfilled = unfulfilled
}

func (r *bootstrapResult) AddResult(other Result) {
	if other == nil {
		return
	}
	r.results.AddResults(other.ShardResults())
	r.unfulfilled.AddRanges(other.Unfulfilled())
}
