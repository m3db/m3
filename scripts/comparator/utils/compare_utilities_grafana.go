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

package utils

import (
	"strings"

	"go.uber.org/zap"
)

// GrafanaQueries is a list of Grafana dashboard compatible queries.
type GrafanaQueries struct {
	// QueryGroup is the general category for these queries.
	QueryGroup string
	// Queries is a list of Grafana dashboard compatible queries.
	Queries []GrafanaQuery
	// Index is this query group's index.
	Index int
}

// GrafanaQuery is a Grafana dashboard compatible query.
type GrafanaQuery struct {
	// Query is the query.
	Query string
	// Interval is the step size.
	Interval string
	// Index is this query's index.
	Index int
	// Left indicates if this panel is on the left.
	Left bool
}

// constructGrafanaQueries constructs a list of Grafana dashboard compatible
// queries.
func (q InputQueries) constructGrafanaQueries() []GrafanaQueries {
	queries := make([]GrafanaQueries, 0, len(q))
	idx := 0
	for _, inQuery := range q {
		query, index := inQuery.constructGrafanaQuery(idx)
		idx = index
		// NB: don't add empty queries if they exist for whatever reason.
		if len(query.Queries) > 0 {
			queries = append(queries, query)
		}
	}

	return queries
}

func (q InputQuery) constructGrafanaQuery(idx int) (GrafanaQueries, int) {
	grafanaQueries := GrafanaQueries{
		QueryGroup: q.QueryGroup,
		Index:      idx,
	}

	queries := make([]GrafanaQuery, 0, len(q.Queries)*len(q.Steps))
	left := true
	for _, inQuery := range q.Queries {
		for _, inStep := range q.Steps {
			idx++
			queries = append(queries, GrafanaQuery{
				Query:    strings.ReplaceAll(inQuery, `"`, `\"`),
				Interval: inStep,
				Index:    idx,
				Left:     left,
			})

			left = !left
		}
	}

	grafanaQueries.Queries = queries
	return grafanaQueries, idx + 1
}

// ParseFileToGrafanaQueries parses a JSON queries file into Grafana dashboard
// compatible queries.
func ParseFileToGrafanaQueries(
	fileName string,
	log *zap.Logger,
) ([]GrafanaQueries, error) {
	queries, err := parseFileToQueries(fileName, log)
	if err != nil {
		return nil, err
	}

	return queries.constructGrafanaQueries(), nil
}
