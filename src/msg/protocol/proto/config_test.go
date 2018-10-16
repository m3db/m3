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

package proto

import (
	"testing"

	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestConfiguration(t *testing.T) {
	str := `
maxMessageSize: 1024
bytesPool:
    buckets:
    - capacity: 4
      count: 60000
    - capacity: 1024
      count: 30000
    watermark:
        lowWatermark: 0.01
        highWatermark: 0.02
`

	var cfg Configuration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))
	opts := cfg.NewOptions(instrument.NewOptions())
	require.Equal(t, 1024, opts.MaxMessageSize())
	require.NotNil(t, opts.BytesPool())
	b := opts.BytesPool().Get(2)
	require.Equal(t, 0, len(b))
	require.Equal(t, 4, cap(b))
	b = opts.BytesPool().Get(200)
	require.Equal(t, 0, len(b))
	require.Equal(t, 1024, cap(b))
}
