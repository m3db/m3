// Copyright (c) 2018 Uber Technologies, Inc.
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

package server

import (
	"time"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

// Configuration configs a server.
type Configuration struct {
	// Server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// Retry mechanism configuration.
	Retry retry.Configuration `yaml:"retry"`

	// Whether keep alives are enabled on connections.
	KeepAliveEnabled *bool `yaml:"keepAliveEnabled"`

	// KeepAlive period.
	KeepAlivePeriod *time.Duration `yaml:"keepAlivePeriod"`
}

// NewOptions creates server options.
func (c Configuration) NewOptions(iOpts instrument.Options) Options {
	opts := NewOptions().
		SetRetryOptions(c.Retry.NewOptions(iOpts.MetricsScope())).
		SetInstrumentOptions(iOpts)
	if c.KeepAliveEnabled != nil {
		opts = opts.SetTCPConnectionKeepAlive(*c.KeepAliveEnabled)
	}
	if c.KeepAlivePeriod != nil {
		opts = opts.SetTCPConnectionKeepAlivePeriod(*c.KeepAlivePeriod)
	}
	return opts
}

// NewServer creates a new server.
func (c Configuration) NewServer(handler Handler, iOpts instrument.Options) Server {
	return NewServer(c.ListenAddress, handler, c.NewOptions(iOpts))
}
