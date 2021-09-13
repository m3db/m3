package promremotewrite

import "time"

// Options for storage.
type Options struct {
	endpoints       []EndpointOptions
	requestTimeout  time.Duration
	connectTimeout  time.Duration
	keepAlive       time.Duration
	idleConnTimeout time.Duration
	maxIdleConns    int
}

// EndpointOptions for single prometheus remote write capable endpoint.
type EndpointOptions struct {
	address    string
	retention  time.Duration
	resolution time.Duration
}
