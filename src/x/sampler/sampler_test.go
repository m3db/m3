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

package sampler

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInvalidSampleRate(t *testing.T) {
	_, err := NewSampler(-1.0)
	require.Error(t, err)

	_, err = NewSampler(0.0)
	require.Error(t, err)

	_, err = NewSampler(1.0)
	require.Error(t, err)

	_, err = NewSampler(2.0)
	require.Error(t, err)
}

func TestSampler(t *testing.T) {
	s, err := NewSampler(0.5)
	require.NoError(t, err)
	require.True(t, s.Sample())
	require.False(t, s.Sample())
	require.True(t, s.Sample())
	require.False(t, s.Sample())
	require.True(t, s.Sample())

	s, err = NewSampler(0.25)
	require.NoError(t, err)
	require.True(t, s.Sample())
	require.False(t, s.Sample())
	require.False(t, s.Sample())
	require.False(t, s.Sample())
	require.True(t, s.Sample())
	require.False(t, s.Sample())
	require.False(t, s.Sample())
	require.False(t, s.Sample())
	require.True(t, s.Sample())
}
