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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var relativeTo = time.Date(2013, time.April, 3, 4, 5, 0, 0, time.UTC)

func TestParseTime(t *testing.T) {

	tests := []struct {
		timespec     string
		expectedTime time.Time
	}{
		{"-4h", relativeTo.Add(-1 * 4 * time.Hour)},
		{"-35MIN", relativeTo.Add(-1 * 35 * time.Minute)},
		{"06:12_07.03.14", time.Date(2014, time.March, 7, 6, 12, 0, 0, time.UTC)},
		{"06:12_03/07/14", time.Date(2014, time.March, 7, 6, 12, 0, 0, time.UTC)},
		{"06:12_140307", time.Date(2014, time.March, 7, 6, 12, 0, 0, time.UTC)},
		{"14:38_20150618", time.Date(2015, time.June, 18, 14, 38, 0, 0, time.UTC)},
		{"07.03.14", time.Date(2014, time.March, 7, 0, 0, 0, 0, time.UTC)},
		{"03/07/14", time.Date(2014, time.March, 7, 0, 0, 0, 0, time.UTC)},
		{"20140307", time.Date(2014, time.March, 7, 0, 0, 0, 0, time.UTC)},
		{"140307", time.Date(2014, time.March, 7, 0, 0, 0, 0, time.UTC)},
		{"1432581620", time.Date(2015, time.May, 25, 19, 20, 20, 0, time.UTC)},
	}

	for _, test := range tests {
		s := test.timespec
		parsed, err := ParseTime(s, relativeTo, 0)
		assert.Nil(t, err, "error parsing %s", s)
		assert.Equal(t, test.expectedTime, parsed, "incorrect parsed value for %s", s)
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		timespec         string
		expectedDuration time.Duration
	}{
		{"-4h", -4 * time.Hour},
		{"-35MIN", -35 * time.Minute},
		{"-10s", -10 * time.Second},
	}

	for _, test := range tests {
		s := test.timespec
		parsed, err := ParseDuration(s)
		assert.Nil(t, err, "error parsing %s", s)
		assert.Equal(t, test.expectedDuration, parsed, "incorrect parsed value for %s", s)
	}
}

func TestParseDurationErrors(t *testing.T) {
	tests := []string{
		"10s",
		"-10.5h",
	}

	for _, test := range tests {
		parsed, err := ParseDuration(test)
		assert.Error(t, err)
		assert.Equal(t, time.Duration(0), parsed)
	}
}

func TestAbsoluteOffset(t *testing.T) {
	tests := []struct {
		timespec     string
		expectedTime time.Time
	}{
		{"-35MIN", relativeTo.Add(-1 * 35 * time.Minute)},
		{"14:12_07.03.14", time.Date(2014, time.March, 7, 7, 12, 0, 0, time.UTC)},
		{"03/07/14", time.Date(2014, time.March, 6, 17, 0, 0, 0, time.UTC)},
	}

	for _, test := range tests {
		s := test.timespec
		parsed, err := ParseTime(s, relativeTo, -7*time.Hour)
		assert.Nil(t, err, "error parsing %s", s)
		assert.Equal(t, test.expectedTime, parsed, "incorrect parsed value for %s", s)
	}
}
