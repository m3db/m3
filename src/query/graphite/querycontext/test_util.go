package querycontext

import (
	"time"
)

// NewTestContext creates a new test context.
func NewTestContext() *Context {
	now := time.Now()
	return NewContext(ContextOptions{Start: now.Add(-time.Hour), End: now})
}
