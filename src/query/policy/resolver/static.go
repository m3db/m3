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

package resolver

import (
	"context"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/tsdb"
	"github.com/m3db/m3metrics/policy"
)

type staticResolver struct {
	sp policy.StoragePolicy
}

// NewStaticResolver creates a static policy resolver.
func NewStaticResolver(sp policy.StoragePolicy) PolicyResolver {
	return &staticResolver{sp: sp}
}

func (r *staticResolver) Resolve(
	// Context needed here to satisfy PolicyResolver interface
	ctx context.Context, // nolint: unparam
	tagMatchers models.Matchers,
	startTime, endTime time.Time,
) ([]tsdb.FetchRequest, error) {
	ranges := tsdb.NewSingleRangeRequest("", startTime, endTime, r.sp).Ranges
	requests := make([]tsdb.FetchRequest, 1)
	tags, err := tagMatchers.ToTags()
	if err != nil {
		return nil, err
	}
	requests[0] = tsdb.FetchRequest{
		ID:     tags.ID(),
		Ranges: ranges,
	}

	return requests, nil
}
