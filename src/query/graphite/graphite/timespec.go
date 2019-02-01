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

package graphite

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/graphite/errors"
)

var reRelativeTime = regexp.MustCompile(`(?i)^\-([0-9]+)(s|min|h|d|w|mon|y)$`)

var periods = map[string]time.Duration{
	"s":   time.Second,
	"min": time.Minute,
	"h":   time.Hour,
	"d":   time.Hour * 24,
	"w":   time.Hour * 24 * 7,
	"mon": time.Hour * 24 * 30,
	"y":   time.Hour * 24 * 365,
}

// on Jan 2 15:04:05 -0700 MST 2006
var formats = []string{
	"15:04_060102",
	"15:04_20060102",
	"15:04_01/02/06",
	"15:04_02.01.06",
	"02.01.06",
	"01/02/06",
	"060102",
	"20060102",
}

// use init to rewrite formats to bypass bug in time.Parse
func init() {
	for i := range formats {
		formats[i] = bypassTimeParseBug(formats[i])
	}
}

func bypassTimeParseBug(s string) string {
	// NB(jayp): Looks like there is a bug in Golang's time.Parse when handing format strings
	// with _2 in the format string. Here is a snippet that exhibits this issue:
	//     t, e := time.Parse("15:04_20060102", "14:38_20150618")
	// We replace underscores with space to bypass this bug.
	return strings.Replace(s, "_", " ", -1)
}

// FormatTime translates a time.Time until a Graphite from/until string
func FormatTime(t time.Time) string {
	return t.Format(formats[0])
}

// ParseTime translates a Graphite from/until string into a timestamp relative to the provide time
func ParseTime(s string, now time.Time, absoluteOffset time.Duration) (time.Time, error) {
	if len(s) == 0 {
		return now, errors.NewInvalidParamsError(fmt.Errorf("time cannot be empty"))
	}

	if s == "now" {
		return now, nil
	}

	if m := reRelativeTime.FindStringSubmatch(s); len(m) != 0 {
		timePast, err := strconv.ParseInt(m[1], 10, 32)
		if err != nil {
			return now, errors.NewInvalidParamsError(fmt.Errorf("invalid relative time %v", err))
		}

		period := periods[strings.ToLower(m[2])]
		return now.Add(-1 * time.Duration(timePast) * period), nil
	}

	newS := bypassTimeParseBug(s)
	for _, format := range formats {
		t, err := time.Parse(format, newS)
		if err == nil {
			// Absolute time passed in, applying offset
			return t.Add(absoluteOffset), nil
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return time.Unix(n, 0).UTC(), nil
	}

	return now, err
}

// ParseDuration parses a duration
func ParseDuration(s string) (time.Duration, error) {
	if m := reRelativeTime.FindStringSubmatch(s); len(m) != 0 {
		timePast, err := strconv.ParseInt(m[1], 10, 32)
		if err != nil {
			return 0, errors.NewInvalidParamsError(fmt.Errorf("invalid relative time %v", err))
		}

		period := periods[strings.ToLower(m[2])]
		return -1 * time.Duration(timePast) * period, nil
	}

	return 0, errors.NewInvalidParamsError(fmt.Errorf("invalid relative time %s", s))
}
