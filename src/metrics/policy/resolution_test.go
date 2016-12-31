// Copyright (c) 2016 Uber Technologies, Inc.
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

package policy

import (
	"testing"
	"time"

	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestValidResolutionValue(t *testing.T) {
	inputs := []ResolutionValue{
		OneSecond,
		TenSeconds,
		OneMinute,
		FiveMinutes,
		TenMinutes,
	}
	expected := []Resolution{
		Resolution{Window: time.Second, Precision: xtime.Second},
		Resolution{Window: 10 * time.Second, Precision: xtime.Second},
		Resolution{Window: time.Minute, Precision: xtime.Minute},
		Resolution{Window: 5 * time.Minute, Precision: xtime.Minute},
		Resolution{Window: 10 * time.Minute, Precision: xtime.Minute},
	}
	for i, value := range inputs {
		resolution, err := value.Resolution()
		require.NoError(t, err)
		require.Equal(t, resolution, expected[i])
		require.True(t, value.IsValid())
	}
}

func TestInvalidResolutionValue(t *testing.T) {
	inputs := []ResolutionValue{
		UnknownResolutionValue,
		ResolutionValue(100),
	}
	for _, value := range inputs {
		_, err := value.Resolution()
		require.Equal(t, errUnknownResolutionValue, err)
		require.False(t, value.IsValid())
	}
}

func TestValidResolution(t *testing.T) {
	inputs := []Resolution{
		{Window: time.Second, Precision: xtime.Second},
		{Window: 10 * time.Second, Precision: xtime.Second},
		{Window: time.Minute, Precision: xtime.Minute},
		{Window: 5 * time.Minute, Precision: xtime.Minute},
		{Window: 10 * time.Minute, Precision: xtime.Minute},
	}
	expected := []ResolutionValue{
		OneSecond,
		TenSeconds,
		OneMinute,
		FiveMinutes,
		TenMinutes,
	}
	for i, input := range inputs {
		resolutionValue, err := ValueFromResolution(input)
		require.NoError(t, err)
		require.Equal(t, expected[i], resolutionValue)
	}
}

func TestInvalidResolution(t *testing.T) {
	inputs := []Resolution{
		{Window: 2 * time.Second, Precision: xtime.Second},
		{Window: time.Second, Precision: xtime.None},
	}
	for _, resolution := range inputs {
		_, err := ValueFromResolution(resolution)
		require.Equal(t, errUnknownResolution, err)
	}
}
