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

package consumer

import (
	"testing"
	"time"

	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestConfiguration(t *testing.T) {
	str := `
messagePool:
  size: 5
  maxBufferReuseSize: 65536
ackFlushInterval: 100ms
ackBufferSize: 100
connectionWriteBufferSize: 200
connectionReadBufferSize: 300
encoder:
  maxMessageSize: 100
  bytesPool:
    watermark:
      low: 0.001
decoder:
  maxMessageSize: 200
  bytesPool:
    watermark:
      high: 0.002
`

	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	opts := cfg.NewOptions(instrument.NewOptions())
	require.Equal(t, 5, opts.MessagePoolOptions().PoolOptions.Size())
	require.Equal(t, 65536, opts.MessagePoolOptions().MaxBufferReuseSize)
	require.Equal(t, 100*time.Millisecond, opts.AckFlushInterval())
	require.Equal(t, 100, opts.AckBufferSize())
	require.Equal(t, 200, opts.ConnectionWriteBufferSize())
	require.Equal(t, 300, opts.ConnectionReadBufferSize())
	require.Equal(t, 100, opts.EncoderOptions().MaxMessageSize())
	require.NotNil(t, opts.EncoderOptions().BytesPool())
	require.Equal(t, 200, opts.DecoderOptions().MaxMessageSize())
	require.NotNil(t, opts.EncoderOptions().BytesPool())
	require.Equal(t, instrument.NewOptions(), opts.InstrumentOptions())
}
