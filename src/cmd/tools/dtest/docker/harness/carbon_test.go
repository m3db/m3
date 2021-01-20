// +build dtest
//
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

package harness

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/tools/dtest/docker/harness/resources"

	"github.com/stretchr/testify/assert"
)

func findVerifier(expected string) resources.ResponseVerifier {
	return func(status int, _ map[string][]string, s string, err error) error {
		if err != nil {
			return err
		}

		if status/100 != 2 {
			return fmt.Errorf("expected 200 status code, got %v", status)
		}

		if s == expected {
			return nil
		}

		return fmt.Errorf("%s does not match expected:%s", s, expected)
	}
}

func renderVerifier(v float64) resources.ResponseVerifier {
	type graphiteRender struct {
		Target     string      `json:"target"`
		Datapoints [][]float64 `json:"datapoints"`
	}

	return func(status int, _ map[string][]string, s string, err error) error {
		if err != nil {
			return err
		}

		if status/100 != 2 {
			return fmt.Errorf("expected 200 status code, got %v", status)
		}

		var render []graphiteRender
		if err := json.Unmarshal([]byte(s), &render); err != nil {
			return err
		}

		if len(render) != 1 {
			return fmt.Errorf("expected one result, got %d", len(render))
		}

		for _, dps := range render[0].Datapoints {
			// NB: graphite presents datapoints as an array [value, timestamp]
			// i.e. value: dps[0], timestamp: dps[1]
			if len(dps) != 2 {
				return fmt.Errorf("expected two values in result, got %d", len(dps))
			}

			if dps[0] == v {
				return nil
			}
		}

		return fmt.Errorf("could not find point with value %f", v)
	}
}

func graphiteQuery(target string, start time.Time) string {
	from := start.Add(time.Minute * -2).Unix()
	until := start.Add(time.Minute * 2).Unix()
	return fmt.Sprintf("api/v1/graphite/render?target=%s&from=%d&until=%d",
		target, from, until)
}

func graphiteFind(query string) string {
	return fmt.Sprintf("api/v1/graphite/metrics/find?query=%s", query)
}

func TestCarbon(t *testing.T) {
	coord := singleDBNodeDockerResources.Coordinator()

	timestamp := time.Now()

	write := func(metric string, value float64) {
		assert.NoError(t, coord.WriteCarbon(7204, metric, value, timestamp))
	}

	read := func(metric string, expected float64) {
		assert.NoError(t, coord.RunQuery(
			renderVerifier(expected),
			graphiteQuery(metric, timestamp)))
	}

	// NB: since carbon writes are aggregated, it might be up to 10 seconds for
	// these points to appear in queries. Because of this, write data for all
	// test cases at once, then query all of them to reduce test duration.
	aggMetric := "foo.min.aggregate.baz"
	write(aggMetric, 41)
	write(aggMetric, 42)
	write(aggMetric, 40)

	unaggregatedMetric := "foo.min.already-aggregated.baz"
	write(unaggregatedMetric, 41)
	write(unaggregatedMetric, 42)

	meanMetric := "foo.min.catch-all.baz"
	write(meanMetric, 10)
	write(meanMetric, 20)

	colonMetric := "foo.min:biz.baz.qux"
	write(colonMetric, 42)

	// Seed some points for find endpoints.
	findMetrics := []string{
		"a",
		"a.bar",
		"a.biz",
		"a.biz.cake",
		"a.bar.caw.daz",
		"a.bag",
		"c:bar.c:baz",
	}

	for _, m := range findMetrics {
		write(m, 0)
	}

	// Test for min aggregation. 40 should win since it has min aggergation.
	read(aggMetric, 40)
	// Test that unaggregated metrics upsert.
	read(unaggregatedMetric, 42)
	// Test that unaggregated metrics upsert.
	read(meanMetric, 15)
	// Test that metrics with colons in them are written correctly.
	read(colonMetric, 42)

	// Test find endpoints with various queries.
	findResults := map[string]string{
		"a.b*.caw.*": `[{"id":"a.b*.caw.daz","text":"daz","leaf":1,"expandable":0,"allowChildren":0}]`,
		"a.b*.c*":    `[{"id":"a.b*.cake","text":"cake","leaf":1,"expandable":0,"allowChildren":0},{"id":"a.b*.caw","text":"caw","leaf":0,"expandable":1,"allowChildren":1}]`,
		"a.b*":       `[{"id":"a.bar","text":"bar","leaf":1,"expandable":0,"allowChildren":0},{"id":"a.bar","text":"bar","leaf":0,"expandable":1,"allowChildren":1},{"id":"a.biz","text":"biz","leaf":1,"expandable":0,"allowChildren":0},{"id":"a.biz","text":"biz","leaf":0,"expandable":1,"allowChildren":1},{"id":"a.bag","text":"bag","leaf":1,"expandable":0,"allowChildren":0}]`,
		"a.ba[rg]":   `[{"id":"a.bag","text":"bag","leaf":1,"expandable":0,"allowChildren":0},{"id":"a.bar","text":"bar","leaf":1,"expandable":0,"allowChildren":0},{"id":"a.bar","text":"bar","leaf":0,"expandable":1,"allowChildren":1}]`,
		"a*":         `[{"id":"a","text":"a","leaf":1,"expandable":0,"allowChildren":0},{"id":"a","text":"a","leaf":0,"expandable":1,"allowChildren":1}]`,
		"c:*":        `[{"id":"c:bar","text":"c:bar","leaf":0,"expandable":1,"allowChildren":1}]`,
		"c:bar.*":    `[{"id":"c:bar.c:baz","text":"c:baz","leaf":1,"expandable":0,"allowChildren":0}]`,
		"x":          "[]",
		"a.d":        "[]",
		"*.*.*.*.*":  "[]",
	}

	for query, ex := range findResults {
		assert.NoError(t, coord.RunQuery(
			findVerifier(ex), graphiteFind(query)))
	}
}
