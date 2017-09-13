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

package retention

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConfigurationAssignment(t *testing.T) {
	var (
		retentionPeriod                       = 1 * time.Hour
		blockSize                             = 2 * time.Hour
		bufferFuture                          = 3 * time.Hour
		bufferPast                            = 4 * time.Hour
		blockDataExpiry                       = true
		blockDataExpiryAfterNotAccessedPeriod = 6 * time.Hour
		config                                = &Configuration{
			RetentionPeriod:                       retentionPeriod,
			BlockSize:                             blockSize,
			BufferFuture:                          bufferFuture,
			BufferPast:                            bufferPast,
			BlockDataExpiry:                       &blockDataExpiry,
			BlockDataExpiryAfterNotAccessedPeriod: &blockDataExpiryAfterNotAccessedPeriod,
		}
	)

	opts := config.Options()
	require.Equal(t, retentionPeriod, opts.RetentionPeriod())
	require.Equal(t, blockSize, opts.BlockSize())
	require.Equal(t, bufferFuture, opts.BufferFuture())
	require.Equal(t, bufferPast, opts.BufferPast())
	require.Equal(t, blockDataExpiry, opts.BlockDataExpiry())
	require.Equal(t, blockDataExpiryAfterNotAccessedPeriod, opts.BlockDataExpiryAfterNotAccessedPeriod())
}
