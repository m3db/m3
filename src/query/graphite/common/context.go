// Copyright (c) 2019 Uber Technologies, Inc.
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

package common

import (
	ctx "context"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
	"github.com/m3db/m3/src/query/storage"
)

// contextBase are the real content of a Context, minus the lock so that we
// can safely copy a context without violating the rules of go vet.
// nolint
type contextBase struct {
	// QueryRaw string.
	QueryRaw string

	// QueryExpression is the top level query expression.
	QueryExpression fmt.Stringer

	// TimeRangeAdjusted is a boolean indicating whether the time range has an adjustment.
	TimeRangeAdjusted bool

	// The start time to query against.
	StartTime time.Time

	// The end time to query against.
	EndTime time.Time

	// TimeRangeAdjustment is the time range adjustment made to the query.
	TimeRangeAdjustment TimeRangeAdjustment

	// The underlying engine.
	Engine QueryEngine

	// Trace records traces.
	Trace Tracer

	// Timeout indicates whether to use a custom timeout when fetching data,
	// specify zero to indicate default timeout or a positive value
	Timeout time.Duration

	// Source is the query source.
	Source []byte

	// MaxDataPoints is the max datapoints for the query.
	MaxDataPoints int64

	// FetchOpts are the fetch options to use for the query.
	FetchOpts *storage.FetchOptions

	parent                   *Context
	reqCtx                   ctx.Context
	storageContext           context.Context
	timeRangeAdjustmentStats TimeRangeAdjustmentStats
}

// Context is the parameters to a query evaluation.
type Context struct {
	sync.RWMutex
	contextBase
}

// ContextOptions provides the options to create the context with
type ContextOptions struct {
	Start         time.Time
	End           time.Time
	Engine        QueryEngine
	Timeout       time.Duration
	MaxDataPoints int64
	FetchOpts     *storage.FetchOptions
}

// TimeRangeAdjustment is an applied time range adjustment.
type TimeRangeAdjustment struct {
	OriginalStart time.Time
	OriginalEnd   time.Time
	ShiftStart    time.Duration
	ShiftEnd      time.Duration
	ExpandStart   time.Duration
	ExpandEnd     time.Duration
}

// NewContext creates a new context.
func NewContext(options ContextOptions) *Context {
	return &Context{
		contextBase: contextBase{
			StartTime:      options.Start,
			EndTime:        options.End,
			Engine:         options.Engine,
			storageContext: context.New(),
			Timeout:        options.Timeout,
			MaxDataPoints:  options.MaxDataPoints,
			FetchOpts:      options.FetchOpts,
		},
	}
}

// TracingEnabled checks whether tracing is enabled for this context.
func (c *Context) TracingEnabled() bool { return c.Trace != nil }

// ChildContextOptions is a set of options to pass when creating a child context.
type ChildContextOptions struct {
	query struct {
		set        bool
		raw        string
		expression fmt.Stringer
	}
	adjustment struct {
		adjusted              bool
		conditionallyAdjusted bool
		shiftStart            time.Duration
		shiftEnd              time.Duration
		expandStart           time.Duration
		expandEnd             time.Duration
	}
}

// NewChildContextOptions returns an initialized ChildContextOptions struct.
func NewChildContextOptions() ChildContextOptions {
	return ChildContextOptions{}
}

// NewChildContextOptions returns an initialized ChildContextOptions struct.
func NewChildContextOptionsWithQuery(
	query string,
	expr fmt.Stringer,
) ChildContextOptions {
	opts := ChildContextOptions{}
	opts.query.set = true
	opts.query.raw = query
	opts.query.expression = expr
	return opts
}

// AdjustTimeRange will adjust the child context's time range.
func (o *ChildContextOptions) AdjustTimeRange(
	shiftStart, shiftEnd, expandStart, expandEnd time.Duration,
) {
	if shiftStart == 0 && shiftEnd == 0 && expandStart == 0 && expandEnd == 0 {
		// Not an adjustment, don't mark "adjusted" true
		return
	}
	o.adjustment.adjusted = true
	o.adjustment.shiftStart = shiftStart
	o.adjustment.shiftEnd = shiftEnd
	o.adjustment.expandStart = expandStart
	o.adjustment.expandEnd = expandEnd
}

func (o *ChildContextOptions) ConditionalAdjustTimeRange(
	shiftStart, shiftEnd, expandStart, expandEnd time.Duration,
) {
	o.AdjustTimeRange(shiftStart, shiftEnd, expandStart, expandEnd)
	if o.adjustment.adjusted {
		o.adjustment.conditionallyAdjusted = true
	}
}

// NewChildContext creates a child context.  Child contexts can have any of
// their parameters modified, but share the same underlying storage context.
func (c *Context) NewChildContext(opts ChildContextOptions) *Context {
	// create a duplicate of the parent context with an independent lock
	// (otherwise `go vet` complains due to the -copylock check)
	c.RLock()
	child := &Context{
		contextBase: c.contextBase,
	}
	child.parent = c
	c.RUnlock()

	origStart, origEnd := child.StartTime, child.EndTime
	if child.TimeRangeAdjusted {
		origStart, origEnd = c.TimeRangeAdjustment.OriginalStart, c.TimeRangeAdjustment.OriginalEnd
	}

	if opts.adjustment.adjusted {
		child.TimeRangeAdjusted = true
		child.TimeRangeAdjustment.OriginalStart = origStart
		child.TimeRangeAdjustment.OriginalEnd = origEnd
		child.TimeRangeAdjustment.ShiftStart += opts.adjustment.shiftStart
		child.TimeRangeAdjustment.ShiftEnd += opts.adjustment.shiftEnd
		child.TimeRangeAdjustment.ExpandStart += opts.adjustment.expandStart
		child.TimeRangeAdjustment.ExpandEnd += opts.adjustment.expandEnd

		child.StartTime = origStart.
			Add(child.TimeRangeAdjustment.ShiftStart).
			Add(-child.TimeRangeAdjustment.ExpandStart)
		child.EndTime = origEnd.
			Add(child.TimeRangeAdjustment.ShiftEnd).
			Add(child.TimeRangeAdjustment.ExpandEnd)

		topContext := c.topContext()
		topContext.Lock()
		topContext.timeRangeAdjustmentStats.Total++
		if opts.adjustment.conditionallyAdjusted {
			topContext.timeRangeAdjustmentStats.ConditionalAdjustments++
		} else {
			topContext.timeRangeAdjustmentStats.UnconditionalAdjustments++
		}
		topContext.Unlock()
	}

	if opts.query.set {
		child.QueryRaw = opts.query.raw
		child.QueryExpression = opts.query.expression
	}

	child.reqCtx = c.reqCtx
	return child
}

func (c *Context) topContext() *Context {
	c.RLock()
	parent := c.parent
	c.RUnlock()
	if parent == nil {
		return c
	}
	return parent.topContext()
}

type TimeRangeAdjustmentStats struct {
	UnconditionalAdjustments int64
	ConditionalAdjustments   int64
	Total                    int64
}

func (c *Context) TimeRangeAdjustmentStats() TimeRangeAdjustmentStats {
	topContext := c.topContext()
	topContext.RLock()
	defer topContext.RUnlock()
	return topContext.timeRangeAdjustmentStats
}

// Close closes the context
func (c *Context) Close() error {
	if c.parent != nil {
		// Closing a child context is meaningless.
		return nil
	}

	return c.storageContext.Close()
}

// SetRequestContext sets the given context as the request context for this
// execution context. This is used for calls to the m3 storage wrapper.
func (c *Context) SetRequestContext(reqCtx ctx.Context) {
	c.Lock()
	c.reqCtx = reqCtx
	c.Unlock()
}

// RequestContext will provide the wrapped request context. Used for calls
// to m3 storage wrapper.
func (c *Context) RequestContext() ctx.Context {
	c.RLock()
	r := c.reqCtx
	c.RUnlock()
	return r
}

// RegisterCloser registers a new Closer with the context.
func (c *Context) RegisterCloser(closer context.Closer) {
	c.storageContext.RegisterCloser(closer)
}

// AddAsyncTasks adds tracked asynchronous task(s)
func (c *Context) AddAsyncTasks(count int) {
	c.storageContext.AddAsyncTasks(count)
}

// DoneAsyncTask marks a single tracked asynchronous task complete
func (c *Context) DoneAsyncTask() {
	c.storageContext.DoneAsyncTask()
}

// A Trace is tracing information about a function or fetch within a query.
type Trace struct {
	// ActivityName is the name of the activity being traced.
	ActivityName string

	// Duration is the amount of time it took to execute the activity.
	Duration time.Duration

	// Inputs are the number of timeseries processed by the trace.
	Inputs []TraceStats

	// Outputs is the number of timeseries returned by the trace.
	Outputs TraceStats
}

// TraceStats tracks the number of timeseries used by a trace.
type TraceStats struct {
	NumSeries int // number of timeseries being acted on
}

// A Tracer is used to record a Trace.
type Tracer func(t Trace)
