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

package client

type TLSOptions interface {
	// SetTLSEnabled sets the TLS enabled option
	SetTLSEnabled(value bool) TLSOptions

	// TLSEnabled returns the TLS enabled option
	TLSEnabled() bool

	// SetInsecureSkipVerify sets the insecure skip verify option
	SetInsecureSkipVerify(value bool) TLSOptions

	// InsecureSkipVerify returns the insecure skip verify option
	InsecureSkipVerify() bool

	// SetServerName sets the server name option
	SetServerName(value string) TLSOptions

	// ServerName returns the server name option
	ServerName() string

	// SetCAFile sets the CA file path
	SetCAFile(value string) TLSOptions

	// CAFile returns the CA file path
	CAFile() string

	// SetCertFile sets the certificate file path
	SetCertFile(value string) TLSOptions

	// CertFile returns the certificate file path
	CertFile() string

	// SetKeyFile sets the key file path
	SetKeyFile(value string) TLSOptions

	// KeyFile returns the key file path
	KeyFile() string
}

type tlsOptions struct {
	tlsEnabled         bool
	insecureSkipVerify bool
	serverName         string
	caFile             string
	certFile           string
	keyFile            string
}

// NewTLSOptions creates new TLS options
func NewTLSOptions() TLSOptions {
	return &tlsOptions{
		tlsEnabled:         false,
		insecureSkipVerify: true,
	}
}

func (o *tlsOptions) SetTLSEnabled(value bool) TLSOptions {
	opts := *o
	opts.tlsEnabled = value
	return &opts
}

func (o *tlsOptions) TLSEnabled() bool {
	return o.tlsEnabled
}

func (o *tlsOptions) SetInsecureSkipVerify(value bool) TLSOptions {
	opts := *o
	opts.insecureSkipVerify = value
	return &opts
}

func (o *tlsOptions) InsecureSkipVerify() bool {
	return o.insecureSkipVerify
}

func (o *tlsOptions) SetServerName(value string) TLSOptions {
	opts := *o
	opts.serverName = value
	return &opts
}

func (o *tlsOptions) ServerName() string {
	return o.serverName
}

func (o *tlsOptions) SetCAFile(value string) TLSOptions {
	opts := *o
	opts.caFile = value
	return &opts
}

func (o *tlsOptions) CAFile() string {
	return o.caFile
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
