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

func TestEqualsTrue(t *testing.T) {
	opts := NewOptions()
	require.True(t, opts.Equal(opts))

	otherOpts := NewOptions()
	require.True(t, opts.Equal(otherOpts))
	require.True(t, otherOpts.Equal(opts))
}

func TestEqualsFalse(t *testing.T) {
	opts := NewOptions()
	otherOpts := NewOptions().SetBlockSize(10 * time.Hour)
	require.False(t, opts.Equal(otherOpts))
	require.False(t, otherOpts.Equal(opts))
}

func TestOptionsGettersAndSetters(t *testing.T) {
	opts := NewOptions()

	retentionPeriod := 24 * time.Hour
	opts = opts.SetRetentionPeriod(retentionPeriod)
	require.Equal(t, retentionPeriod, opts.RetentionPeriod())

	futureRetentionPeriod := 12 * time.Hour
	opts = opts.SetFutureRetentionPeriod(futureRetentionPeriod)
	require.Equal(t, futureRetentionPeriod, opts.FutureRetentionPeriod())

	blockSize := 2 * time.Hour
	opts = opts.SetBlockSize(blockSize)
	require.Equal(t, blockSize, opts.BlockSize())

	bufferFuture := 1 * time.Hour
	opts = opts.SetBufferFuture(bufferFuture)
	require.Equal(t, bufferFuture, opts.BufferFuture())

	bufferPast := 1 * time.Hour
	opts = opts.SetBufferPast(bufferPast)
	require.Equal(t, bufferPast, opts.BufferPast())

	opts = opts.SetBlockDataExpiry(true)
	require.True(t, opts.BlockDataExpiry())
	opts = opts.SetBlockDataExpiry(false)
	require.False(t, opts.BlockDataExpiry())

	expiryPeriod := 30 * time.Minute
	opts = opts.SetBlockDataExpiryAfterNotAccessedPeriod(expiryPeriod)
	require.Equal(t, expiryPeriod, opts.BlockDataExpiryAfterNotAccessedPeriod())
}

func TestOptionsValidate(t *testing.T) {
	opts := NewOptions()

	err := opts.Validate()
	require.NoError(t, err)

	opts = NewOptions().SetBufferFuture(-1 * time.Hour)
	err = opts.Validate()
	require.Error(t, err)

	opts = NewOptions().SetBufferPast(-1 * time.Hour)
	err = opts.Validate()
	require.Error(t, err)

	opts = NewOptions().SetBlockSize(0)
	err = opts.Validate()
	require.Error(t, err)

	opts = NewOptions().SetBlockSize(-1 * time.Hour)
	err = opts.Validate()
	require.Error(t, err)

	opts = NewOptions().
		SetBlockSize(1 * time.Hour).
		SetBufferFuture(2 * time.Hour)
	err = opts.Validate()
	require.Error(t, err)

	opts = NewOptions().
		SetBlockSize(1 * time.Hour).
		SetBufferPast(2 * time.Hour)
	err = opts.Validate()
	require.Error(t, err)

	opts = NewOptions().
		SetBlockSize(2 * time.Hour).
		SetRetentionPeriod(1 * time.Hour)
	err = opts.Validate()
	require.Error(t, err)
}

func TestOptionsEdgeCases(t *testing.T) {
	opts := NewOptions()

	opts = opts.
		SetRetentionPeriod(0).
		SetBlockSize(0).
		SetFutureRetentionPeriod(0).
		SetBufferFuture(0).
		SetBufferPast(0).
		SetBlockDataExpiryAfterNotAccessedPeriod(0)

	require.Equal(t, time.Duration(0), opts.RetentionPeriod())
	require.Equal(t, time.Duration(0), opts.BlockSize())
	require.Equal(t, time.Duration(0), opts.FutureRetentionPeriod())
	require.Equal(t, time.Duration(0), opts.BufferFuture())
	require.Equal(t, time.Duration(0), opts.BufferPast())
	require.Equal(t, time.Duration(0), opts.BlockDataExpiryAfterNotAccessedPeriod())
}
