// Copyright (c) 2024 Uber Technologies, Inc.
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

	xtime "github.com/m3db/m3/src/x/time"
)

func TestFlushTimeStart(t *testing.T) {
	opts := NewOptions().
		SetRetentionPeriod(24 * time.Hour).
		SetBlockSize(2 * time.Hour)

	now := xtime.Now()
	expected := now.Add(-24 * time.Hour).Truncate(2 * time.Hour)

	actual := FlushTimeStart(opts, now)
	require.Equal(t, expected, actual)
}

func TestFlushTimeStartForRetentionPeriod(t *testing.T) {
	retentionPeriod := 24 * time.Hour
	blockSize := 2 * time.Hour
	now := xtime.Now()

	expected := now.Add(-retentionPeriod).Truncate(blockSize)
	actual := FlushTimeStartForRetentionPeriod(retentionPeriod, blockSize, now)

	require.Equal(t, expected, actual)
}

func TestFlushTimeEnd(t *testing.T) {
	opts := NewOptions().
		SetBlockSize(2 * time.Hour).
		SetFutureRetentionPeriod(12 * time.Hour).
		SetBufferPast(1 * time.Hour)

	now := xtime.Now()
	expected := now.Add(12 * time.Hour).Add(-1 * time.Hour).Add(-2 * time.Hour).Truncate(2 * time.Hour)

	actual := FlushTimeEnd(opts, now)
	require.Equal(t, expected, actual)
}

func TestFlushTimeEndForBlockSize(t *testing.T) {
	blockSize := 2 * time.Hour
	now := xtime.Now()

	expected := now.Add(-blockSize).Truncate(blockSize)
	actual := FlushTimeEndForBlockSize(blockSize, now)

	require.Equal(t, expected, actual)
}

func TestFlushTimeEdgeCases(t *testing.T) {
	opts := NewOptions().
		SetRetentionPeriod(0).
		SetBlockSize(0).
		SetFutureRetentionPeriod(0).
		SetBufferPast(0)

	now := xtime.Now()

	FlushTimeStart(opts, now)
	FlushTimeEnd(opts, now)

	opts = NewOptions().
		SetRetentionPeriod(-24 * time.Hour).
		SetBlockSize(-2 * time.Hour)

	FlushTimeStart(opts, now)
}
