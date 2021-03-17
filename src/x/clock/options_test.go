// Copyright (c) 2021 Uber Technologies, Inc.
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

package clock

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPanicOnDefaultClock(t *testing.T) {
	tests := []struct {
		value       string
		shouldPanic bool
	}{
		{
			value:       "<unset>", // environment variable not set
			shouldPanic: false,
		},
		{
			value:       "",
			shouldPanic: false,
		},
		{
			value:       "false",
			shouldPanic: false,
		},
		{
			value:       "0",
			shouldPanic: false,
		},
		{
			value:       "true",
			shouldPanic: true,
		},
		{
			value:       "1",
			shouldPanic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			if tt.value != "<unset>" {
				require.NoError(t, os.Setenv(panicOnDefaultClockEnvVar, tt.value))
			}
			nowFn := NewOptions().NowFn()
			require.NoError(t, os.Unsetenv(panicOnDefaultClockEnvVar))
			if tt.shouldPanic {
				assert.Panics(t, func() {
					nowFn()
				})
			} else {
				assert.NotPanics(t, func() {
					nowFn()
				})
			}
		})
	}
}
