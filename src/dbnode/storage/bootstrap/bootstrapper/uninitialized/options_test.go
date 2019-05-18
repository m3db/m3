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

package uninitialized

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name        string
		modifier    func(opts Options) Options
		expectedErr error
	}{
		{
			name: "default valid",
			modifier: func(opts Options) Options {
				return opts
			},
		},
		{
			name: "no result options",
			modifier: func(opts Options) Options {
				return opts.SetResultOptions(nil)
			},
			expectedErr: errNoResultOptions,
		},
		{
			name: "no instrument options",
			modifier: func(opts Options) Options {
				return opts.SetInstrumentOptions(nil)
			},
			expectedErr: errNoInstrumentOptions,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := test.modifier(NewOptions())
			err := opts.Validate()
			if test.expectedErr != nil {
				require.Error(t, err)
				require.Equal(t, test.expectedErr, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
