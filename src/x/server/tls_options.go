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

package server

// TLSOptions provide a set of TLS options
type TLSOptions interface {
	// SetMode sets the tls mode
	SetMode(value TLSMode) TLSOptions
	// Mode returns the tls mode
	Mode() TLSMode

	// SetMutualTLSEnabled sets the mutual tls enabled option
	SetMutualTLSEnabled(value bool) TLSOptions
	// MutualTLSEnabled returns the mutual tls enabled option
	MutualTLSEnabled() bool

	// SetCertFile sets the certificate file path
	SetCertFile(value string) TLSOptions
	// CertFile returns the certificate file path
	CertFile() string

	// SetKeyFile sets the private key file path
	SetKeyFile(value string) TLSOptions
	// KeyFile returns the private key file path
	KeyFile() string

	// SetClientCAFile sets the CA file path
	SetClientCAFile(value string) TLSOptions
	// ClientCAFile returns the CA file path
	ClientCAFile() string
}

type tlsOptions struct {
	mode         TLSMode
	mTLSEnabled  bool
	certFile     string
	keyFile      string
	clientCAFile string
}

// NewTLSOptions creates a new set of tls options
func NewTLSOptions() TLSOptions {
	return &tlsOptions{
		mode:        TLSDisabled,
		mTLSEnabled: false,
	}
}

func (o *tlsOptions) SetMode(value TLSMode) TLSOptions {
	opts := *o
	opts.mode = value
	return &opts
}

func (o *tlsOptions) Mode() TLSMode {
	return o.mode
}

func (o *tlsOptions) SetMutualTLSEnabled(value bool) TLSOptions {
	opts := *o
	opts.mTLSEnabled = value
	return &opts
}

func (o *tlsOptions) MutualTLSEnabled() bool {
	return o.mTLSEnabled
}

func (o *tlsOptions) SetCertFile(value string) TLSOptions {
	opts := *o
	opts.certFile = value
	return &opts
}

func (o *tlsOptions) CertFile() string {
	return o.certFile
}

func (o *tlsOptions) SetKeyFile(value string) TLSOptions {
	opts := *o
	opts.keyFile = value
	return &opts
}

func (o *tlsOptions) KeyFile() string {
	return o.keyFile
}

func (o *tlsOptions) SetClientCAFile(value string) TLSOptions {
	opts := *o
	opts.clientCAFile = value
	return &opts
}

func (o *tlsOptions) ClientCAFile() string {
	return o.clientCAFile
}
