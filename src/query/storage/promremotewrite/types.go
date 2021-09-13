package promremotewrite

import "time"

type Options struct {
	endpoints       []EndpointOptions
	requestTimeout  time.Duration
	connectTimeout  time.Duration
	keepAlive       time.Duration
	idleConnTimeout time.Duration
	maxIdleConns    int
}

type EndpointOptions struct {
	address    string
	retention  time.Duration
	resolution time.Duration
}