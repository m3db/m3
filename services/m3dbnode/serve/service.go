package serve

// NetworkService is a network service that can serve remote clients
type NetworkService interface {
	// ListenAndServe will listen and serve traffic, returns a close and any error that occurred
	ListenAndServe() (Close, error)
}

// Close is a method to call to close a resource or procedure
type Close func()
