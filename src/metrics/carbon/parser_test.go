// Copyright (c) 2019 Uber Technologies, Inc.
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

package carbon

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	nullTime time.Time
)

type carbonLine struct {
	path  string
	value float64
	time  time.Time
	line  string
}

var testLines = []carbonLine{
	{"foo.bar.zed", 45565.02, time.Unix(1428951394, 0),
		"foo.bar.zed 45565.02 1428951394"},
	{"foo.bar.quad", 10.0, time.Unix(1428951394, 0),
		"foo.bar.quad  10 1428951394"},
	{"foo.bar.nan", math.NaN(), time.Unix(1428951394, 0),
		"foo.bar.nan nan  1428951394"},
	{"foo.bar.nan", math.NaN(), time.Unix(1428951394, 0),
		"foo.bar.nan -NaN 1428951394"},
	{"foo.bar.negative", -18000.00000, time.Unix(1429480924, 0),
		"foo.bar.negative -18000.000000 1429480924"},
}

func TestScannerMetric(t *testing.T) {
	var buf bytes.Buffer
	for _, line := range testLines {
		fmt.Fprintf(&buf, "%s\n", line.line)
	}

	s := NewScanner(&buf)
	for _, line := range testLines {
		t.Run(line.path, func(t *testing.T) {
			require.True(t, s.Scan(), "could not parse to line %s, err: %v", line.line, s.Err())
			name, ts, value := s.Metric()
			expectedName, expectedTime, expectedValue :=
				line.path, line.time, line.value
			assert.Equal(t, expectedName, string(name))
			assert.Equal(t, expectedTime, ts)
			if math.IsNaN(expectedValue) {
				require.True(t, math.IsNaN(value))
			} else {
				assert.Equal(t, expectedValue, value)
			}
		})
	}

	assert.False(t, s.Scan(), "parsed past end of buffer")
	assert.Nil(t, s.Err())
	assert.Equal(t, 0, s.MalformedCount)
}

func TestParse(t *testing.T) {
	for i := range testLines {
		name, ts, value, err := Parse([]byte(testLines[i].line))
		require.Nil(t, err, "could not parse %s", testLines[i].line)
		assert.Equal(t, testLines[i].path, string(name))
		assert.Equal(t, testLines[i].time, ts)

		if math.IsNaN(testLines[i].value) {
			require.True(t, math.IsNaN(value))
		} else {
			assert.Equal(t, testLines[i].value, value)
		}

	}
}

func TestParseName(t *testing.T) {
	for _, line := range testLines {
		name, rest, err := ParseName([]byte(line.line))
		_ = rest
		assert.Equal(t, line.path, string(name))
		// we expect the returned string to have de-duped spaces after ParseName's
		// initial pass
		exp := strings.TrimPrefix(line.line[len(line.path)+1:], " ")
		assert.Equal(t, exp, string(rest))
		assert.Nil(t, err)
	}

	_, _, err := ParseName([]byte("thereisnospacehere"))
	assert.NotNil(t, err)
}

func TestParseErrors(t *testing.T) {
	assertParseError(t, " ")
	assertParseError(t, "  ")
	assertParseError(t, "   ")
	assertParseError(t, "a ")
	assertParseError(t, "a 1")
	assertParseError(t, "a  ")
	assertParseError(t, "a 1 ")
	assertParseError(t, "a  1")
	assertParseError(t, " 1 1")
	assertParseError(t, "  1")
	assertParseError(t, "foo")
	assertParseError(t, "foo bar zed")
	assertParseError(t, "foo 4394 zed")
	assertParseError(t, "foo zed 1428951394")
	assertParseError(t, "foo 4394")
	assertParseError(t, "foo 4384 1428951394 bar")
	assertParseError(t, "foo 4384 1428951394 1428951394 bar")
}

func TestParsePacket(t *testing.T) {
	mets, malformed := ParsePacket([]byte(`
foo.bar.zed 45565.02 1428951394
foo.bar.zed NaN 1428951394
foo.bar.invalid invalid 1428951394

foo.bar.invalid 1428951394`))
	require.Equal(t, 2, len(mets))
	require.Equal(t, 2, malformed)
}

func TestCarbonToLine(t *testing.T) {
	validateLine(t, "foo.bar.zed 45565.02 1428951394")
	validateLine(t, "foo.bar.nan NaN 1428951395")
}

func TestParseValidatesUTF8Encoding(t *testing.T) {
	start := "foo.bar.baz"
	end := "some-other-foo-.bar.baz 4394 1428951394"

	var b []byte
	b = append(b, []byte(start)...)
	b = append(b, []byte{byte(255), byte(253)}...)
	b = append(b, []byte(end)...)

	_, _, _, err := Parse(b)
	if err == nil {
		t.Fatalf("expected UTF8 error")
	}
	assert.Equal(t, "not valid UTF8 string", err.Error())
	validateLineError(t, string(b))
}

func validateLine(t *testing.T, line string) {
	mets, malformed := ParsePacket([]byte(line))
	require.Equal(t, 0, malformed)
	require.Equal(t, line+"\n", mets[0].ToLine())
}

func validateLineError(t *testing.T, line string) {
	_, malformed := ParsePacket([]byte(line))
	require.Equal(t, 1, malformed)
}

func assertParseError(t *testing.T, metric string) {
	_, _, _, err := Parse([]byte(metric))
	assert.NotNil(t, err, "allowed parsing of %s", metric)
}
