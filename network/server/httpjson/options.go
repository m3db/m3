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

	apachethrift "github.com/apache/thrift/lib/go/thrift"
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
	// ReadTimeout sets the read timeout and returns a new ServerOptions
	ReadTimeout(value time.Duration) ServerOptions

	// GetReadTimeout returns the read timeout
	GetReadTimeout() time.Duration

	// WriteTimeout sets the write timeout and returns a new ServerOptions
	WriteTimeout(value time.Duration) ServerOptions

	// GetWriteTimeout returns the write timeout
	GetWriteTimeout() time.Duration

	// RequestTimeout sets the request timeout and returns a new ServerOptions
	RequestTimeout(value time.Duration) ServerOptions

	// GetRequestTimeout returns the request timeout
	GetRequestTimeout() time.Duration

	// ContextFn sets the context fn and returns a new ServerOptions
	ContextFn(value ContextFn) ServerOptions

	// GetContextFn returns the context fn
	GetContextFn() ContextFn

	// PostResponseFn sets the post response fn and returns a new ServerOptions
	PostResponseFn(value PostResponseFn) ServerOptions

	// GetPostResponseFn returns the post response fn
	GetPostResponseFn() PostResponseFn
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

func (o *serverOptions) ContextFn(value ContextFn) ServerOptions {
	opts := *o
	opts.contextFn = value
	return &opts
}

func (o *serverOptions) GetContextFn() ContextFn {
	return o.contextFn
}

func (o *serverOptions) PostResponseFn(value PostResponseFn) ServerOptions {
	opts := *o
	opts.postResponseFn = value
	return &opts
}

func (o *serverOptions) GetPostResponseFn() PostResponseFn {
	return o.postResponseFn
}
