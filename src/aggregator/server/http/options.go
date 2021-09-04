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

package http

import (
	"net/http"
	"time"
)

const (
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
)

// Options is a set of server options.
type Options interface {
	// SetReadTimeout sets the read timeout.
	SetReadTimeout(value time.Duration) Options

	// ReadTimeout returns the read timeout.
	ReadTimeout() time.Duration

	// SetWriteTimeout sets the write timeout.
	SetWriteTimeout(value time.Duration) Options

	// WriteTimeout returns the write timeout.
	WriteTimeout() time.Duration

	// Mux returns the http mux for the server.
	Mux() *http.ServeMux

	// SetMux sets the http mux for the server.
	// This option exists to allow overriding in tests. Prod code should not set this and use the
	// http.DefaultServerMux instead. std pkgs like pprof assume the default mux.
	SetMux(value *http.ServeMux) Options
}

type options struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	mux          *http.ServeMux
}

// NewOptions creates a new set of server options.
func NewOptions() Options {
	return &options{
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		mux:          http.NewServeMux(),
	}
}

func (o *options) SetReadTimeout(value time.Duration) Options {
	opts := *o
	opts.readTimeout = value
	return &opts
}

func (o *options) ReadTimeout() time.Duration {
	return o.readTimeout
}

func (o *options) SetWriteTimeout(value time.Duration) Options {
	opts := *o
	opts.writeTimeout = value
	return &opts
}

func (o *options) WriteTimeout() time.Duration {
	return o.writeTimeout
}

func (o *options) Mux() *http.ServeMux {
	return o.mux
}

func (o *options) SetMux(value *http.ServeMux) Options {
	opts := *o
	opts.mux = value
	return &opts
}
