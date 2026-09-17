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

package encoding

import (
	"math"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumSig(t *testing.T) {
	require.Equal(t, uint8(0), NumSig(uint64(0)))
	require.Equal(t, uint8(1), NumSig(uint64(1)))
	require.Equal(t, uint8(4), NumSig(uint64(12)))
	require.Equal(t, uint8(63), NumSig(uint64(math.MaxUint64>>1)))
	require.Equal(t, uint8(64), NumSig(uint64(math.MaxUint64)))
	require.Equal(t, uint8(64), NumSig(uint64(math.MaxUint64-1)))
}

func TestLeadingAndTrailingZeros(t *testing.T) {
	tests := []struct {
		name       string
		input      uint64
		expectedLZ int
		expectedTZ int
	}{
		{"Zero case", 0, 64, 0},
		{"All ones", ^uint64(0), 0, 0},
		{"Single bit (LSB)", 1, 63, 0},
		{"Single bit (MSB)", 1 << 63, 0, 63},
		{"Multiple bits", 0b0000110000000000, 52, 10},
		{"Random number", 0xF0F00000000000F, bits.LeadingZeros64(0xF0F00000000000F), bits.TrailingZeros64(0xF0F00000000000F)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leading, trailing := LeadingAndTrailingZeros(tt.input)
			require.Equal(t, tt.expectedLZ, leading)
			require.Equal(t, tt.expectedTZ, trailing)
		})
	}
}
