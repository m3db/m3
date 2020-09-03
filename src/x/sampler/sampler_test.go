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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestInvalidSampleRate(t *testing.T) {
	_, err := NewSampler(-1.0)
	require.Error(t, err)

	_, err = NewSampler(0.0)
	require.NoError(t, err)

	_, err = NewSampler(1.0)
	require.NoError(t, err)

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

func TestRateUnmarshalYAML(t *testing.T) {
	type config struct {
		Rate Rate `yaml:"rate"`
	}

	tests := []struct {
		input       []byte
		expectValue float64
		expectErr   bool
	}{
		{
			input:       []byte("rate: foo\n"),
			expectValue: 0,
			expectErr:   true,
		},
		{
			input:       []byte("rate: -1\n"),
			expectValue: 0,
			expectErr:   true,
		},
		{
			input:       []byte("\n"),
			expectValue: 0,
			expectErr:   false,
		},
		{
			input:       []byte("rate: 0\n"),
			expectValue: 0,
			expectErr:   false,
		},
		{
			input:       []byte("rate: 1\n"),
			expectValue: 1,
			expectErr:   false,
		},
		{
			input:       []byte("rate: 1.01\n"),
			expectValue: 0,
			expectErr:   true,
		},
	}

	for _, test := range tests {
		t.Run(string(test.input), func(t *testing.T) {
			var c config
			err := yaml.Unmarshal(test.input, &c)
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, test.expectValue, c.Rate.Value())
		})
	}
}
