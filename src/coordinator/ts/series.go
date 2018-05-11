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

package ts

import (
	"context"
	"time"

	"github.com/m3db/m3db/src/coordinator/models"
)

// A Series is the public interface to a block of timeseries values.  Each block has a start time,
// a logical number of steps, and a step size indicating the number of milliseconds represented by each point.
type Series struct {
	name      string
	startTime time.Time
	vals      Values
	ctx       context.Context

	// The Specification is the path that was used to generate this timeseries,
	// typically either the query, or the function stack used to transform
	// specific results.
	Specification string

	// Metric tags.
	Tags models.Tags
}

// NewSeries creates a new Series at a given start time, backed by the provided values
func NewSeries(ctx context.Context, name string, startTime time.Time, vals Values, tags models.Tags) *Series {
	return &Series{
		name:          name,
		startTime:     startTime,
		vals:          vals,
		ctx:           ctx,
		Specification: name,
		Tags:          tags,
	}
}

// StartTime returns the time the block starts
func (b *Series) StartTime() time.Time { return b.startTime }

// Name returns the name of the timeseries block
func (b *Series) Name() string { return b.name }

// Len returns the number of values in the time series. Used for aggregation
func (b *Series) Len() int { return b.vals.Len() }

// ValueAt returns the value at a given step. Used for aggregation
func (b *Series) ValueAt(i int) float64 { return b.vals.ValueAt(i) }

// MillisPerStep returns the step size for the vals
func (b *Series) MillisPerStep() int { return b.vals.MillisPerStep() }

// StartTimeForStep returns the time at which the given step starts
func (b *Series) StartTimeForStep(n int) time.Time {
	return b.StartTime().Add(time.Millisecond * time.Duration(n*b.vals.MillisPerStep()))
}
