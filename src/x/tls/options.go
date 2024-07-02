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

package tls

import "time"

// Options provide a set of TLS options
type Options interface {
	// SetClientEnabled sets the TLS enabled option
	SetClientEnabled(value bool) Options
	// ClientEnabled returns the TLS enabled option
	ClientEnabled() bool

	// SetInsecureSkipVerify sets the insecure skip verify option
	SetInsecureSkipVerify(value bool) Options
	// InsecureSkipVerify returns the insecure skip verify option
	InsecureSkipVerify() bool

	// SetServerName sets the server name option
	SetServerName(value string) Options
	// ServerName returns the server name option
	ServerName() string

	// SetServerMode sets the tls mode
	SetServerMode(value ServerMode) Options
	// Mode returns the tls mode
	ServerMode() ServerMode

	// SetMutualTLSEnabled sets the mutual tls enabled option
	SetMutualTLSEnabled(value bool) Options
	// MutualTLSEnabled returns the mutual tls enabled option
	MutualTLSEnabled() bool

	// SetCertFile sets the certificate file path
	SetCertFile(value string) Options
	// CertFile returns the certificate file path
	CertFile() string

	// SetKeyFile sets the private key file path
	SetKeyFile(value string) Options
	// KeyFile returns the private key file path
	KeyFile() string

	// SetCAFile sets the CA file path
	SetCAFile(value string) Options
	// CAFile returns the CA file path
	CAFile() string

	// SetCertificatesTTL sets the certificates TTL
	SetCertificatesTTL(value time.Duration) Options
	// CertificatesTTL returns the certificates TTL
	CertificatesTTL() time.Duration
}

type options struct {
	clientEnabled      bool
	insecureSkipVerify bool
	serverName         string
	serverMode         ServerMode
	mTLSEnabled        bool
	certFile           string
	keyFile            string
	caFile             string
	certificatesTTL    time.Duration
}

// NewOptions creates a new set of tls options
func NewOptions() Options {
	return &options{
		clientEnabled:      false,
		insecureSkipVerify: true,
		serverMode:         Disabled,
		mTLSEnabled:        false,
	}
}

func (o *options) SetClientEnabled(value bool) Options {
	opts := *o
	opts.clientEnabled = value
	return &opts
}

func (o *options) ClientEnabled() bool {
	return o.clientEnabled
}

func (o *options) SetInsecureSkipVerify(value bool) Options {
	opts := *o
	opts.insecureSkipVerify = value
	return &opts
}

func (o *options) InsecureSkipVerify() bool {
	return o.insecureSkipVerify
}

func (o *options) SetServerName(value string) Options {
	opts := *o
	opts.serverName = value
	return &opts
}

func (o *options) ServerName() string {
	return o.serverName
}

func (o *options) SetServerMode(value ServerMode) Options {
	opts := *o
	opts.serverMode = value
	return &opts
}

func (o *options) ServerMode() ServerMode {
	return o.serverMode
}

func (o *options) SetMutualTLSEnabled(value bool) Options {
	opts := *o
	opts.mTLSEnabled = value
	return &opts
}

func (o *options) MutualTLSEnabled() bool {
	return o.mTLSEnabled
}

func (o *options) SetCertFile(value string) Options {
	opts := *o
	opts.certFile = value
	return &opts
}

func (o *options) CertFile() string {
	return o.certFile
}

func (o *options) SetKeyFile(value string) Options {
	opts := *o
	opts.keyFile = value
	return &opts
}

func (o *options) KeyFile() string {
	return o.keyFile
}

func (o *options) SetCAFile(value string) Options {
	opts := *o
	opts.caFile = value
	return &opts
}

func (o *options) CAFile() string {
	return o.caFile
}

func (o *options) SetCertificatesTTL(value time.Duration) Options {
	opts := *o
	opts.certificatesTTL = value
	return &opts
}

func (o *options) CertificatesTTL() time.Duration {
	return o.certificatesTTL
}
