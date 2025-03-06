package server

type StoppableServer interface {
	Server
	Stop()
}

func NewStoppableServer(
	address string,
	handler Handler,
	opts Options,
) StoppableServer {
	return newServer(address, handler, opts)
}
