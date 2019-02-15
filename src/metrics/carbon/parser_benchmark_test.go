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
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	testCarbonLine       = []byte("foo.bar.baz 0 1441317285")
	testCarbonLineSpaces = []byte("foo.bar.baz    0  1441317285")
)

var (
	// nolint: gosimple
	reCarbon = regexp.MustCompile("(?i)^([^\\s]+)\\s+(-?[0-9\\.]+|\\-?nan)\\s+([0-9]+)\\s*$")
)

func BenchmarkParse(b *testing.B) {
	for n := 0; n < b.N; n++ {
		Parse(testCarbonLine)
	}
}

func BenchmarkParseSpaces(b *testing.B) {
	for n := 0; n < b.N; n++ {
		Parse(testCarbonLineSpaces)
	}
}

func BenchmarkRegex(b *testing.B) {
	for n := 0; n < b.N; n++ {
		regexParse(string(testCarbonLine))
	}
}

func BenchmarkParseName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ParseName(testCarbonLine)
	}
}

func BenchmarkParsePacket(b *testing.B) {
	var test string
	for i := 0; i < 10; i++ {
		test += string(testCarbonLine) + "\n"
	}
	testBytes := []byte(test)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParsePacket(testBytes)
	}
}

// regexParse parses a carbon line into the corresponding parts using regex
func regexParse(line string) (name string, timestamp time.Time, value float64, err error) {
	parts := reCarbon.FindStringSubmatch(line)
	if len(parts) == 0 {
		err = errInvalidLine
		return
	}

	name = parts[1]
	tsInSecs, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		err = fmt.Errorf("invalid timestamp %s: %v", parts[3], err)
		return
	}

	timestamp = time.Unix(tsInSecs, 0)
	if strings.ToLower(parts[2]) == "-nan" || strings.ToLower(parts[2]) == "nan" {
		value = math.NaN()
	} else {
		value, err = strconv.ParseFloat(parts[2], 64)
	}

	return
}
