package memtsdb

// Closer closes a resource
type Closer func()

// Context provides context to an operation
type Context interface {
	// RegisterCloser will register a resource closer
	RegisterCloser(closer Closer)

	// DependsOn will register a blocking context that
	// must complete first before closers can be called
	DependsOn(blocker Context)

	// Close will close the current context
	Close()

	// Reset will reset the context for reuse
	Reset()
}
