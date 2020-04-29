// Copyright (c) 2020 Uber Technologies, Inc.
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

package net

import (
	gonet "net"

	"github.com/valyala/tcplisten"
)

var (
	reusePortConfig = &tcplisten.Config{
		ReusePort:   true,
		DeferAccept: true,
		FastOpen:    true,
	}
)

// ListenerOptions is a set of listener options that
// can create listeners.
type ListenerOptions struct {
	reusePort bool
}

// ListenerOption is a listener option that can
// be applied to a set of listener options.
type ListenerOption func(o *ListenerOptions)

// ListenerReusePort is a listener option to reuse ports.
func ListenerReusePort(value bool) ListenerOption {
	return func(o *ListenerOptions) {
		o.reusePort = true
	}
}

// NewListenerOptions creates a set of listener options
// with supplied options.
func NewListenerOptions(opts ...ListenerOption) ListenerOptions {
	var result ListenerOptions
	for _, fn := range opts {
		fn(&result)
	}
	return result
}

// Listen creates a new listener based on options.
func (o ListenerOptions) Listen(
	protocol, address string,
) (gonet.Listener, error) {
	if o.reusePort {
		if protocol == "tcp" {
			protocol = "tcp4"
		}
		return reusePortConfig.NewListener(protocol, address)
	}
	return gonet.Listen(protocol, address)
}
