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

package config

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/msg/producer/buffer"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestBufferConfiguration(t *testing.T) {
	str := `
onFullStrategy: returnError
maxBufferSize: 100
maxMessageSize: 16
closeCheckInterval: 3s
scanBatchSize: 128
dropOldestInterval: 500ms
allowedSpilloverRatio: 0.1
cleanupRetry:
  initialBackoff: 2s
`

	var cfg BufferConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	bOpts := cfg.NewOptions(instrument.NewOptions())
	require.Equal(t, buffer.ReturnError, bOpts.OnFullStrategy())
	require.Equal(t, 100, bOpts.MaxBufferSize())
	require.Equal(t, 16, bOpts.MaxMessageSize())
	require.Equal(t, 3*time.Second, bOpts.CloseCheckInterval())
	require.Equal(t, 128, bOpts.ScanBatchSize())
	require.Equal(t, 500*time.Millisecond, bOpts.DropOldestInterval())
	require.Equal(t, 0.1, bOpts.AllowedSpilloverRatio())
	require.Equal(t, 2*time.Second, bOpts.CleanupRetryOptions().InitialBackoff())
}

func TestEmptyBufferConfiguration(t *testing.T) {
	var cfg BufferConfiguration
	require.NoError(t, yaml.Unmarshal(nil, &cfg))
	require.Equal(t, BufferConfiguration{}, cfg)
	rOpts := retry.NewOptions()
	require.Equal(t,
		buffer.NewOptions().SetCleanupRetryOptions(rOpts),
		cfg.NewOptions(instrument.NewOptions()).SetCleanupRetryOptions(rOpts),
	)
}
