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

	apachethrift "github.com/uber/tchannel-go/thirdparty/github.com/apache/thrift/lib/go/thrift"
	"github.com/uber/tchannel-go/thrift"
	"golang.org/x/net/context"
)

const (
	defaultReadTimeout    = 10 * time.Second
	defaultWriteTimeout   = 10 * time.Second
	defaultRequestTimeout = 60 * time.Second
)

// ContextFn is a function that sets the context for all service
// methods derived from the incoming request context
type ContextFn func(ctx context.Context, method string, headers map[string]string) thrift.Context

// PostResponseFn is a function that is called at the end of a request
type PostResponseFn func(ctx context.Context, method string, response apachethrift.TStruct)

// ServerOptions is a set of server options
type ServerOptions interface {
	// SetReadTimeout sets the read timeout and returns a new ServerOptions
	SetReadTimeout(value time.Duration) ServerOptions

	// ReadTimeout returns the read timeout
	ReadTimeout() time.Duration

	// SetWriteTimeout sets the write timeout and returns a new ServerOptions
	SetWriteTimeout(value time.Duration) ServerOptions

	// WriteTimeout returns the write timeout
	WriteTimeout() time.Duration

	// SetRequestTimeout sets the request timeout and returns a new ServerOptions
	SetRequestTimeout(value time.Duration) ServerOptions

	// RequestTimeout returns the request timeout
	RequestTimeout() time.Duration

	// SetContextFn sets the context fn and returns a new ServerOptions
	SetContextFn(value ContextFn) ServerOptions

	// ContextFn returns the context fn
	ContextFn() ContextFn

	// SetPostResponseFn sets the post response fn and returns a new ServerOptions
	SetPostResponseFn(value PostResponseFn) ServerOptions

	// PostResponseFn returns the post response fn
	PostResponseFn() PostResponseFn
}

type serverOptions struct {
	readTimeout    time.Duration
	writeTimeout   time.Duration
	requestTimeout time.Duration
	contextFn      ContextFn
	postResponseFn PostResponseFn
}

// NewServerOptions creates a new set of server options with defaults
func NewServerOptions() ServerOptions {
	return &serverOptions{
		readTimeout:    defaultReadTimeout,
		writeTimeout:   defaultWriteTimeout,
		requestTimeout: defaultRequestTimeout,
	}
}

func (o *serverOptions) SetReadTimeout(value time.Duration) ServerOptions {
	opts := *o
	opts.readTimeout = value
	return &opts
}

func (o *serverOptions) ReadTimeout() time.Duration {
	return o.readTimeout
}

func (o *serverOptions) SetWriteTimeout(value time.Duration) ServerOptions {
	opts := *o
	opts.writeTimeout = value
	return &opts
}

func (o *serverOptions) WriteTimeout() time.Duration {
	return o.writeTimeout
}

func (o *serverOptions) SetRequestTimeout(value time.Duration) ServerOptions {
	opts := *o
	opts.requestTimeout = value
	return &opts
}

func (o *serverOptions) RequestTimeout() time.Duration {
	return o.requestTimeout
}

func (o *serverOptions) SetContextFn(value ContextFn) ServerOptions {
	opts := *o
	opts.contextFn = value
	return &opts
}

func (o *serverOptions) ContextFn() ContextFn {
	return o.contextFn
}

func (o *serverOptions) SetPostResponseFn(value PostResponseFn) ServerOptions {
	opts := *o
	opts.postResponseFn = value
	return &opts
}

func (o *serverOptions) PostResponseFn() PostResponseFn {
	return o.postResponseFn
}
