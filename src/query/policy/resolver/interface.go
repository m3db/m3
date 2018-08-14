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
)

// PolicyResolver resolves policy for a query.
type PolicyResolver interface {
	// Resolve will resolve each metric ID to a FetchRequest with a list of FetchRanges.
	// The list of ranges is guaranteed to cover the full [startTime, endTime). The best
	// storage policy will be picked for the range with configured strategy, but there
	// may still be no data retained for the range in the given storage policy.
	Resolve(
		ctx context.Context,
		tagMatchers models.Matchers,
		startTime, endTime time.Time,
	) ([]tsdb.FetchRequest, error)
}
