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

	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/retry"
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

	// TLS configuration
	TLS *TLSConfiguration `yaml:"tls"`
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
	if c.TLS != nil {
		opts = opts.SetTLSOptions(c.TLS.NewOptions())
	}
	return opts
}

// NewServer creates a new server.
func (c Configuration) NewServer(handler Handler, iOpts instrument.Options) Server {
	return NewServer(c.ListenAddress, handler, c.NewOptions(iOpts))
}

// TLSConfiguration configs a tls server
type TLSConfiguration struct {
	// Mode is the tls server mode
	// disabled - allows plaintext connections only
	// permissive - allows both plaintext and TLS connections
	// enforced - allows TLS connections only
	Mode string `yaml:"mode" validate:"nonzero,regexp=^(disabled|permissive|enforced)$"`

	// MutualTLSEnabled sets mTLS
	MutualTLSEnabled bool `yaml:"mTLSEnabled"`

	// CertFile path to a server certificate file
	CertFile string `yaml:"certFile"`

	// KeyFile path to a server key file
	KeyFile string `yaml:"keyFile"`

	// ClientCAFile path to a CA file for verifying clients
	ClientCAFile string `yaml:"clientCAFile"`

	// CertificatesTTL is a time duration certificates are stored in memory
	CertificatesTTL time.Duration `yaml:"certificatesTTL"`
}

// NewOptions creates TLS options
func (c TLSConfiguration) NewOptions() TLSOptions {
	opts := NewTLSOptions().
		SetMutualTLSEnabled(c.MutualTLSEnabled).
		SetCertFile(c.CertFile).
		SetKeyFile(c.KeyFile).
		SetClientCAFile(c.ClientCAFile).
		SetCertificatesTTL(c.CertificatesTTL)
	var tlsMode TLSMode
	if err := tlsMode.UnmarshalText([]byte(c.Mode)); err == nil {
		opts = opts.SetMode(tlsMode)
	}
	return opts
}
