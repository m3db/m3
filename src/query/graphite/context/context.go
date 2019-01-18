package context

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Closer is an interface implemented by objects that should be closed
// when a context completes
type Closer interface {
	Close() error
}

// Context is an execution context.  A Context holds deadlines, cancellation
// signals, and request scoped values.  Contexts should be scoped to a request;
// created when the request begins and closed when the request completes.
// Contexts are safe for use by multiple goroutines, but should not span
// multiple requests.
type Context interface {
	// TODO(mmihic): Integrate this with golang/x/net/context
	Closer

	// Cancel will signal cancellation of the current work for this context. The
	// cancel is idempotent and can be repeated safely multiple times.
	Cancel()

	// IsCancelled will return whether a signal cancellation for this work has been issued.
	IsCancelled() bool

	// RegisterCloser registers an object that should be closed when this
	// context is closed.  Can be used to cleanup per-request objects.
	RegisterCloser(closer Closer)

	// AddAsyncTask allows asynchronous tasks to be enqueued that will
	// ensure this context does not call its registered closers until
	// the tasks are all complete
	AddAsyncTasks(count int)

	// DoneAsyncTask signals that an asynchronous task is complete, when
	// all asynchronous tasks complete if the context has been closed and
	// avoided calling its registered closers it will finally call them
	DoneAsyncTask()

	// AddWarning adds a warning error for methods to pass along to
	// clients regarding overall operation.
	AddWarning(message error)

	// Warnings returns all current warnings for this context, must return
	// a copy since warnings can be added asynchronously
	Warnings() []error

	// ClearWarnings clears all warnings stored for this context
	ClearWarnings()

	// SetConfidence sets the confidence of the datapoints returned by a query
	SetConfidence(score float64) error

	// Confidence returns the confidence of the datapoints returned by a query
	Confidence() float64
}

// New creates a new context
func New() Context {
	return &context{
		warnings:   make(map[error]int),
		confidence: 1.0,
	}
}

type contextStatus int

const (
	contextStatusOpen contextStatus = iota
	contextStatusClosed
)

type context struct {
	sync.RWMutex
	closers    []Closer
	status     contextStatus
	asyncTasks int
	warnings   map[error]int
	cancelled  uint32
	confidence float64
}

// Close closes the context
func (c *context) Close() error {
	finalize := false

	c.Lock()
	if c.status == contextStatusOpen {
		if c.asyncTasks == 0 {
			finalize = true
		}
		c.status = contextStatusClosed
	}
	c.Unlock()

	if finalize {
		return c.callClosers()
	}
	return nil
}

// Cancel cancels the work for this context
func (c *context) Cancel() {
	atomic.StoreUint32(&c.cancelled, 1)
}

// IsCancelled returns whether the work for this context has been cancelled
func (c *context) IsCancelled() bool {
	return atomic.LoadUint32(&c.cancelled) == 1
}

// RegisterCloser registers a new Closer with the context
func (c *context) RegisterCloser(closer Closer) {
	c.Lock()
	c.closers = append(c.closers, closer)
	c.Unlock()
}

// AddAsyncTasks adds tracked asynchronous task(s)
func (c *context) AddAsyncTasks(count int) {
	c.Lock()
	c.asyncTasks += count
	c.Unlock()
}

// DoneAsyncTask marks a single tracked asynchronous task complete
func (c *context) DoneAsyncTask() {
	finalize := false

	c.Lock()
	c.asyncTasks--
	if c.asyncTasks == 0 && c.status == contextStatusClosed {
		finalize = true
	}
	c.Unlock()

	if finalize {
		c.callClosers()
	}
}

func (c *context) callClosers() error {
	var firstErr error
	c.RLock()
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil {
			//FIXME: log.Errorf("could not close %v: %v", closer, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	c.RUnlock()
	return firstErr
}

func (c *context) AddWarning(err error) {
	c.Lock()
	if _, ok := c.warnings[err]; ok {
		c.Unlock()
		// Don't reinsert or else it index of insertions will be off
		return
	}
	idx := len(c.warnings)
	c.warnings[err] = idx
	c.Unlock()
}

func (c *context) Warnings() []error {
	c.RLock()
	if len(c.warnings) == 0 {
		// Default case of no warnings avoids any allocs
		c.RUnlock()
		return nil
	}
	// Return in order by using the index for when it was inserted
	// so values can be reliably tested in order in tests
	warnings := make([]error, len(c.warnings))
	for warning, idx := range c.warnings {
		warnings[idx] = warning
	}
	c.RUnlock()
	return warnings
}

func (c *context) ClearWarnings() {
	c.Lock()
	c.warnings = make(map[error]int)
	c.Unlock()
}

func (c *context) SetConfidence(score float64) error {
	c.Lock()
	defer c.Unlock()
	if score < 0.0 || score > 1.0 {
		return fmt.Errorf("score must be between 0 and 1, instead got: %f", score)
	}
	if score < c.confidence {
		c.confidence = score
	}
	return nil
}

func (c *context) Confidence() float64 {
	c.RLock()
	confidence := c.confidence
	c.RUnlock()

	return confidence
}
