package context

import (
	"sync"

	"code.uber.internal/infra/memtsdb"
)

const (
	defaultClosersCapacity = 4
)

type dependency struct {
	closers      []memtsdb.Closer
	dependencies sync.WaitGroup
}

// NB(r): using golang.org/x/net/context is too GC expensive
type ctx struct {
	sync.RWMutex
	pool   memtsdb.ContextPool
	closed bool
	dep    *dependency
}

// NewContext creates a new context
func NewContext() memtsdb.Context {
	return NewPooledContext(nil)
}

// NewPooledContext returns a new context that is returned to a pool when closed
func NewPooledContext(pool memtsdb.ContextPool) memtsdb.Context {
	return &ctx{pool: pool}
}

func (c *ctx) ensureDependencies() {
	if c.dep != nil {
		return
	}
	// TODO(r): return these to a pool on reset, otherwise over time
	// all contexts in a shared pool will acquire a dependency object
	c.dep = &dependency{
		closers: make([]memtsdb.Closer, 0, defaultClosersCapacity),
	}
}

func (c *ctx) RegisterCloser(closer memtsdb.Closer) {
	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.ensureDependencies()
	c.dep.closers = append(c.dep.closers, closer)
	c.Unlock()
}

func (c *ctx) DependsOn(blocker memtsdb.Context) {
	c.Lock()
	closed := c.closed
	if !closed {
		c.ensureDependencies()
		c.dep.dependencies.Add(1)
	}
	c.Unlock()

	if !closed {
		blocker.RegisterCloser(func() {
			c.dep.dependencies.Done()
		})
	}
}

func (c *ctx) Close() {
	var closers []memtsdb.Closer

	c.Lock()
	if c.closed {
		c.Unlock()
		return
	}
	c.closed = true
	if c.dep != nil {
		closers = c.dep.closers[:]
	}
	c.Unlock()

	if len(closers) > 0 {
		go func() {
			// Wait for dependencies
			c.dep.dependencies.Wait()
			// Now call closers
			for _, closer := range closers {
				closer()
			}
			c.returnToPool()
		}()
		return
	}

	c.returnToPool()
}

func (c *ctx) Reset() {
	c.Lock()
	c.closed = false
	if c.dep != nil {
		c.dep.closers = c.dep.closers[:0]
		c.dep.dependencies = sync.WaitGroup{}
	}
	c.Unlock()
}

func (c *ctx) returnToPool() {
	if c.pool != nil {
		c.Reset()
		c.pool.Put(c)
	}
}
