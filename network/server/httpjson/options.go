// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package httpjson

import (
	"time"
)

const (
	defaultReadTimeout    = 10 * time.Second
	defaultWriteTimeout   = 10 * time.Second
	defaultRequestTimeout = 60 * time.Second
)

// ServerOptions is a set of server options
type ServerOptions interface {
	// ReadTimeout sets the readTimeout and returns a new ServerOptions
	ReadTimeout(value time.Duration) ServerOptions

	// GetReadTimeout returns the readTimeout
	GetReadTimeout() time.Duration

	// WriteTimeout sets the writeTimeout and returns a new ServerOptions
	WriteTimeout(value time.Duration) ServerOptions

	// GetWriteTimeout returns the writeTimeout
	GetWriteTimeout() time.Duration

	// RequestTimeout sets the requestTimeout and returns a new ServerOptions
	RequestTimeout(value time.Duration) ServerOptions

	// GetRequestTimeout returns the requestTimeout
	GetRequestTimeout() time.Duration
}

type serverOptions struct {
	readTimeout    time.Duration
	writeTimeout   time.Duration
	requestTimeout time.Duration
}

// NewServerOptions creates a new set of server options with defaults
func NewServerOptions() ServerOptions {
	return &serverOptions{
		readTimeout:    defaultReadTimeout,
		writeTimeout:   defaultWriteTimeout,
		requestTimeout: defaultRequestTimeout,
	}
}

func (o *serverOptions) ReadTimeout(value time.Duration) ServerOptions {
	opts := *o
	opts.readTimeout = value
	return &opts
}

func (o *serverOptions) GetReadTimeout() time.Duration {
	return o.readTimeout
}

func (o *serverOptions) WriteTimeout(value time.Duration) ServerOptions {
	opts := *o
	opts.writeTimeout = value
	return &opts
}

func (o *serverOptions) GetWriteTimeout() time.Duration {
	return o.writeTimeout
}

func (o *serverOptions) RequestTimeout(value time.Duration) ServerOptions {
	opts := *o
	opts.requestTimeout = value
	return &opts
}

func (o *serverOptions) GetRequestTimeout() time.Duration {
	return o.requestTimeout
}
