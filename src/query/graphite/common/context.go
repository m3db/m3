package common

import (
	ctx "context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3/src/query/graphite/context"
)

// contextBase are the real content of a Context, minus the lock so that we
// can safely copy a context without violating the rules of go vet.
// nolint
type contextBase struct {
	// TimeRangeAdjusted is a boolean indicating whether the time range has an adjustment.
	TimeRangeAdjusted bool

	// LocalOnly determines whether the query fetches data from the local dc only
	LocalOnly bool

	// UseCache indicates whether the query should fetch data from the cache
	UseCache bool

	// UseM3DB indicates whether the query should fetch data from M3DB
	UseM3DB bool

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

	// Zone determines whether the query is restricted to a zone
	Zone string

	parent          *Context
	reqCtx          ctx.Context
	storageContext  context.Context
	state           map[string]interface{}
	cancelled       uint32
	fetchBreakdowns []FetchBreakdown
}

// Context is the parameters to a query evaluation.
type Context struct {
	sync.RWMutex
	contextBase
}

// ContextOptions provides the options to create the context with
type ContextOptions struct {
	Start     time.Time
	End       time.Time
	Engine    QueryEngine
	LocalOnly bool
	UseCache  bool
	UseM3DB   bool
	Timeout   time.Duration
	Zone      string
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
			LocalOnly:      options.LocalOnly,
			UseCache:       options.UseCache,
			UseM3DB:        options.UseM3DB,
			Timeout:        options.Timeout,
			Zone:           options.Zone,
		},
	}
}

// Cancel cancels the work for this context
func (c *Context) Cancel() {
	atomic.StoreUint32(&c.cancelled, 1)
}

// IsCancelled returns whether the work for this context has been cancelled
func (c *Context) IsCancelled() bool {
	result := atomic.LoadUint32(&c.cancelled) == 1
	if result {
		return true
	}
	if c.parent != nil {
		return c.parent.IsCancelled()
	}
	return false
}

// TracingEnabled checks whether tracing is enabled for this context.
func (c *Context) TracingEnabled() bool { return c.Trace != nil }

// ChildContextOptions is a set of options to pass when creating a child context.
type ChildContextOptions struct {
	adjustment struct {
		adjusted    bool
		shiftStart  time.Duration
		shiftEnd    time.Duration
		expandStart time.Duration
		expandEnd   time.Duration
	}
}

// NewChildContextOptions returns an initialized ChildContextOptions struct.
func NewChildContextOptions() ChildContextOptions {
	return ChildContextOptions{}
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
		if opts.adjustment.expandStart > child.TimeRangeAdjustment.ExpandStart {
			child.TimeRangeAdjustment.ExpandStart = opts.adjustment.expandStart
		}
		if opts.adjustment.expandEnd > child.TimeRangeAdjustment.ExpandEnd {
			child.TimeRangeAdjustment.ExpandEnd = opts.adjustment.expandEnd
		}

		child.StartTime = origStart.
			Add(child.TimeRangeAdjustment.ShiftStart).
			Add(-child.TimeRangeAdjustment.ExpandStart)
		child.EndTime = origEnd.
			Add(child.TimeRangeAdjustment.ShiftEnd).
			Add(child.TimeRangeAdjustment.ExpandEnd)
	}

	return child
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

// Set sets a variable for all parent and child contexts
func (c *Context) Set(name string, value interface{}) {
	if c.parent != nil {
		c.parent.Set(name, value)
		return
	}

	c.Lock()
	c.setOrCreateState()
	c.state[name] = value
	c.Unlock()
}

// Get gets a variable from the root parent
func (c *Context) Get(name string) interface{} {
	if c.parent != nil {
		return c.parent.Get(name)
	}
	c.RLock()
	c.setOrCreateState()
	val := c.state[name]
	c.RUnlock()
	return val
}

// AddWarning adds a warning error for methods to pass along to
// clients regarding overall operation.
func (c *Context) AddWarning(err error) {
	c.storageContext.AddWarning(err)
}

// Warnings returns all current warnings for this context
func (c *Context) Warnings() []error {
	return c.storageContext.Warnings()
}

// ClearWarnings clears all warnings stored for this context
func (c *Context) ClearWarnings() {
	c.storageContext.ClearWarnings()
}

// Confidence returns the confidence of the datapoints returned by a query
func (c *Context) Confidence() float64 {
	return c.storageContext.Confidence()
}

// SetConfidence sets the confidence of the datapoints returned by a query
func (c *Context) SetConfidence(values float64) error {
	return c.storageContext.SetConfidence(values)
}

func (c *Context) setOrCreateState() {
	if c.state == nil {
		c.state = make(map[string]interface{})
	}
}

// AddFetchBreakdown adds a fetch breakdown information in the context
func (c *Context) AddFetchBreakdown(breakdown FetchBreakdown) {
	// Bubble to parents
	if c.parent != nil {
		c.parent.AddFetchBreakdown(breakdown)
	}
	c.Lock()
	// Note: make sure to append only to avoid having to return a copy in the accessor
	c.fetchBreakdowns = append(c.fetchBreakdowns, breakdown)
	c.Unlock()
}

// FetchBreakdowns returns the fetch breakdowns
func (c *Context) FetchBreakdowns() []FetchBreakdown {
	var snapshot []FetchBreakdown
	c.RLock()
	if length := len(c.fetchBreakdowns); length > 0 {
		snapshot = c.fetchBreakdowns[:length]
	}
	c.RUnlock()
	return snapshot
}

// FetchBreakdown is a set of fetch invocation metadata
type FetchBreakdown struct {
	NumSeries   int
	CompletedAt time.Time
	LocalOnly   bool
}

// Equal returns true if and only if breakdown has
// exact same attributes as other breakdown
func (b FetchBreakdown) Equal(value FetchBreakdown) bool {
	return b.NumSeries == value.NumSeries &&
		b.CompletedAt.Equal(value.CompletedAt) &&
		b.LocalOnly == value.LocalOnly
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
// NB(mmihic): This is a struct so that we can extend it with more stats down
// the line, such as the approx. number of datapoints being processed
type TraceStats struct {
	NumSeries int // number of timeseries being acted on
}

// A Tracer is used to record a Trace.
type Tracer func(t Trace)
