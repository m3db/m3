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

	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestServerConfiguration(t *testing.T) {
	str := `
listenAddress: addr
keepAliveEnabled: true
keepAlivePeriod: 5s
`

	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
	require.Equal(t, "addr", cfg.ListenAddress)
	require.True(t, *cfg.KeepAliveEnabled)
	require.Equal(t, 5*time.Second, *cfg.KeepAlivePeriod)

	opts := cfg.NewOptions(instrument.NewOptions())
	require.Equal(t, 5*time.Second, opts.TCPConnectionKeepAlivePeriod())
	require.True(t, opts.TCPConnectionKeepAlive())

	require.NotNil(t, cfg.NewServer(nil, instrument.NewOptions()))
}
