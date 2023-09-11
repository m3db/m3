// Copyright (c) 2020 Uber Technologies, Inc.
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

package transformation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReset(t *testing.T) {
	reset, err := Reset.UnaryMultiOutputTransform()
	require.NoError(t, err)
	now := time.Now()
	dp := Datapoint{Value: 1000, TimeNanos: now.UnixNano()}
	resolution := 30 * time.Second
	this, other := reset.Evaluate(dp, resolution)
	require.Equal(t, dp, this)
	require.Equal(t, Datapoint{Value: 0, TimeNanos: now.Add(15 * time.Second).UnixNano()}, other)
}

func TestResetAtLeast1Nanosecond(t *testing.T) {
	reset, err := Reset.UnaryMultiOutputTransform()
	require.NoError(t, err)
	now := time.Now()
	dp := Datapoint{Value: 1000, TimeNanos: now.UnixNano()}
	resolution := 1 * time.Nanosecond
	this, other := reset.Evaluate(dp, resolution)
	require.Equal(t, dp, this)
	require.Equal(t, Datapoint{Value: 0, TimeNanos: now.Add(1 * time.Nanosecond).UnixNano()}, other)
}

func TestUnaryMultiParse(t *testing.T) {
	parsed, err := ParseType("Reset")
	require.NoError(t, err)
	require.Equal(t, Reset, parsed)
}
