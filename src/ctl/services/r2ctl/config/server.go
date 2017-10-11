// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"time"

	"github.com/m3db/m3ctl/services/r2ctl/server"
	"github.com/m3db/m3x/instrument"
)

type serverConfig struct {
	// Host is the host name the HTTP server shoud listen on.
	Host string `yaml:"host" validate:"nonzero"`

	// Port is the port the HTTP server should listen on.
	Port int `yaml:"port"`

	// ReadTimeout is the HTTP server read timeout.
	ReadTimeout time.Duration `yaml:"readTimeout"`

	// WriteTimeout HTTP server write timeout.
	WriteTimeout time.Duration `yaml:"writeTimeout"`
}

func (c *serverConfig) NewHTTPServerOptions(
	instrumentOpts instrument.Options,
) server.Options {
	opts := server.NewOptions().SetInstrumentOptions(instrumentOpts)
	if c.ReadTimeout != 0 {
		opts = opts.SetReadTimeout(c.ReadTimeout)
	}
	if c.WriteTimeout != 0 {
		opts = opts.SetWriteTimeout(c.WriteTimeout)
	}

	return opts
}
