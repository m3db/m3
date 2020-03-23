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
	"fmt"
	"sort"
	"strings"
	"time"
)

// Series is a flat JSON serieazeable representation of the series.
type Series struct {
	id string

	Start      time.Time  `json:"start"`
	End        time.Time  `json:"end"`
	Tags       Tags       `json:"tags"`
	Datapoints Datapoints `json:"datapoints"`
}

// Tags is a simple JSON serieazeable representation of tags.
type Tags map[string]string

// Datapoints is a JSON serializeable list of values for the series.
type Datapoints []Datapoint

// Datapoint is a JSON serializeable datapoint for the series.
type Datapoint struct {
	Value     float64   `json:"val"`
	Timestamp time.Time `json:"ts"`
}

// GetOrGenID gets the ID for this result.
func (r *Series) GetOrGenID() string {
	if len(r.id) == 0 {
		tags := make(sort.StringSlice, len(r.Tags))
		for k, v := range r.Tags {
			tags = append(tags, fmt.Sprintf("%s:%s,", k, v))
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
