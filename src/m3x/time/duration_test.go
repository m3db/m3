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

package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExtendedDuration(t *testing.T) {
	const day = 24 * time.Hour
	tests := []struct {
		text string
		d    time.Duration
	}{
		{"6mon1w3d5min2s", day*180 + day*7 + day*3 + 5*time.Minute + 2*time.Second},
		{"334us", 334 * time.Microsecond},
		{"5m4ms", 5*time.Minute + 4*time.Millisecond},
		{"1y6mon", 365*day + 180*day},
		{"245ns", 245 * time.Nanosecond},
	}

	for _, test := range tests {
		d, err := ParseExtendedDuration(test.text)
		require.NoError(t, err, "encountered parse error for %s", test.text)
		assert.Equal(t, test.d, d, "incorrect duration for %s", test.text)
	}
}

func TestParseExtendedDurationErrors(t *testing.T) {
	tests := []struct {
		text string
		err  string
	}{
		{"4", "invalid duration 4, no unit"},
		{"", "duration empty"},
		{"4minutes", "invalid duration 4minutes, invalid unit minutes"},
		{"4d3", "invalid duration 4d3, no unit"},
		{",3490", "invalid duration ,3490, no value specified"},
	}

	for _, test := range tests {
		_, err := ParseExtendedDuration(test.text)
		require.Error(t, err, "expected error for '%s'", test.text)
		assert.Equal(t, test.err, err.Error(), "invalid error for %s", test.text)
	}
}
