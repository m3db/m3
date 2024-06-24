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
	"testing"
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestServerConfiguration(t *testing.T) {
	str := `
listenAddress: addr
keepAliveEnabled: true
keepAlivePeriod: 5s
tls:
  mode: enforced
  mTLSEnabled: true
  certFile: /tmp/cert
  keyFile: /tmp/key
  clientCAFile: /tmp/ca
  certificatesTTL: 10m
`

	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
	require.Equal(t, "addr", cfg.ListenAddress)
	require.True(t, *cfg.KeepAliveEnabled)
	require.Equal(t, 5*time.Second, *cfg.KeepAlivePeriod)

	require.Equal(t, "enforced", cfg.TLS.Mode)
	require.True(t, cfg.TLS.MutualTLSEnabled)
	require.Equal(t, "/tmp/cert", cfg.TLS.CertFile)
	require.Equal(t, "/tmp/key", cfg.TLS.KeyFile)
	require.Equal(t, "/tmp/ca", cfg.TLS.ClientCAFile)
	require.Equal(t, 10*time.Minute, cfg.TLS.CertificatesTTL)

	opts := cfg.NewOptions(instrument.NewOptions())
	require.Equal(t, 5*time.Second, opts.TCPConnectionKeepAlivePeriod())
	require.True(t, opts.TCPConnectionKeepAlive())

	require.Equal(t, TLSEnforced, opts.TLSOptions().Mode())
	require.True(t, opts.TLSOptions().MutualTLSEnabled())
	require.Equal(t, "/tmp/cert", opts.TLSOptions().CertFile())
	require.Equal(t, "/tmp/key", opts.TLSOptions().KeyFile())
	require.Equal(t, "/tmp/ca", opts.TLSOptions().ClientCAFile())
	require.Equal(t, 10*time.Minute, opts.TLSOptions().CertificatesTTL())

	require.NotNil(t, cfg.NewServer(nil, instrument.NewOptions()))
}
