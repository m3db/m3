// Copyright (c) 2020 Uber Technologies, Inc.
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

package parser

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Series is a flat JSON serializable representation of the series.
type Series struct {
	id string

	Start      time.Time  `json:"start"`
	End        time.Time  `json:"end"`
	Tags       Tags       `json:"tags"`
	Datapoints Datapoints `json:"datapoints"`
}

// Tag is a simple JSON serializable representation of a tag.
type Tag [2]string

// NewTag creates a new tag with a given name and value.
func NewTag(name, value string) Tag {
	return Tag{name, value}
}

// Name returns the tag name.
func (t Tag) Name() string {
	return t[0]
}

// Value returns the tag value.
func (t Tag) Value() string {
	return t[1]
}

// Tags is a simple JSON serializable representation of tags.
type Tags []Tag

// Get returns a list of tag values with the given name.
func (t Tags) Get(name string) []string {
	// NB: this is almost always going to be 0
	values := make([]string, 0, 2)
	// NB: This list isn't expected to get very long so it uses array lookup.
	// If this is a problem in the future, `Tags` be converted to a map.
	for _, t := range t {
		if t.Name() == name {
			values = append(values, t.Value())
		}
	}

	return values
}

// Datapoints is a JSON serializeable list of values for the series.
type Datapoints []Datapoint

// Datapoint is a JSON serializeable datapoint for the series.
type Datapoint struct {
	Value     Value     `json:"val"`
	Timestamp time.Time `json:"ts"`
}

// Value is a JSON serizlizable float64 that allows NaNs.
type Value float64

// MarshalJSON returns state as the JSON encoding of a Value.
func (v Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%g", float64(v)))
}

// UnmarshalJSON unmarshals JSON-encoded data into a Value.
func (v *Value) UnmarshalJSON(data []byte) error {
	var str string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return err
	}

	f, err := strconv.ParseFloat(str, 64)
	if err != nil {
		return err
	}

	*v = Value(f)
	return nil
}

// IDOrGenID gets the ID for this result.
func (r *Series) IDOrGenID() string {
	if len(r.id) == 0 {
		tags := make(sort.StringSlice, 0, len(r.Tags))
		for _, v := range r.Tags {
			tags = append(tags, fmt.Sprintf("%s:%s,", v[0], v[1]))
		}

		sort.Sort(tags)
		var sb strings.Builder
		for _, t := range tags {
			sb.WriteString(t)
		}

		r.id = sb.String()
	}

	return r.id
}
