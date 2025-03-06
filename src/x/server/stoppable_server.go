package server

// StoppableServer is essentially a Server with a Stop method
// that can be invoked to first stop the server from accepting new
// connections. This basically means that the server will stop listening
// on the address it was started with. The server will continue to serve
// existing connections until they are closed or the server is closed in which
// case a Close() should be called.
type StoppableServer interface {
	Server
	Stop()
}

// NewStoppableServer creates a new stoppable server.
func NewStoppableServer(
	address string,
	handler Handler,
	opts Options,
) StoppableServer {
	return newServer(address, handler, opts)
}
