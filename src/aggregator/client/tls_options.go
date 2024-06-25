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

import "time"

type TLSOptions interface {
	// SetEnabled sets the TLS enabled option
	SetEnabled(value bool) TLSOptions

	// Enabled returns the TLS enabled option
	Enabled() bool

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

	// SetCertificatesTTL sets the certificates TTL
	SetCertificatesTTL(value time.Duration) TLSOptions

	// CertificatesTTL returns the certificates TTL
	CertificatesTTL() time.Duration
}

type tlsOptions struct {
	enabled            bool
	insecureSkipVerify bool
	serverName         string
	caFile             string
	certFile           string
	keyFile            string
	certificatesTTL    time.Duration
}

// NewTLSOptions creates new TLS options
func NewTLSOptions() TLSOptions {
	return &tlsOptions{
		enabled:            false,
		insecureSkipVerify: true,
	}
}

func (o *tlsOptions) SetEnabled(value bool) TLSOptions {
	opts := *o
	opts.enabled = value
	return &opts
}

func (o *tlsOptions) Enabled() bool {
	return o.enabled
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

func (o *tlsOptions) SetCertificatesTTL(value time.Duration) TLSOptions {
	opts := *o
	opts.certificatesTTL = value
	return &opts
}

func (o *tlsOptions) CertificatesTTL() time.Duration {
	return o.certificatesTTL
}
