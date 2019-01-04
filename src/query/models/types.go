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

package models

import (
	"regexp"
)

// Separators for tags.
const (
	sep = byte(',')
	eq  = byte('=')
)

// TagOptions describes additional options for tags.
type TagOptions interface {
	// Validate validates these tag options.
	Validate() error

	// SetMetricName sets the name for the `metric name` metric.
	SetMetricName(metricName []byte) TagOptions

	// MetricName gets the name for the `metric name `metric`.
	MetricName() []byte
}

// Tags represents a set of tags with options.
type Tags struct {
	Opts TagOptions
	Tags []Tag
}

// Tag is a key/value metric tag pair.
type Tag struct {
	Name  []byte
	Value []byte
}

// MatchType is an enum for label matching types.
type MatchType int

// Possible MatchTypes.
const (
	MatchEqual MatchType = iota
	MatchNotEqual
	MatchRegexp
	MatchNotRegexp
)

// Matcher models the matching of a label.
// NB: when serialized to JSON, name and value will be in base64.
type Matcher struct {
	Type  MatchType `json:"type"`
	Name  []byte    `json:"name"`
	Value []byte    `json:"value"`

	re *regexp.Regexp
}

// Matchers is a list of individual matchers.
type Matchers []Matcher

// Metric is the individual metric that gets returned from the search endpoint.
type Metric struct {
	ID   []byte
	Tags Tags
}

// Metrics is a list of individual metrics.
type Metrics []Metric
