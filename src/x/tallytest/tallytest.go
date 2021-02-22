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

package tallytest

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

// AssertCounterValue asserts that the given counter has the expected value.
func AssertCounterValue(t *testing.T, expected int64, s tally.Snapshot, name string, tags map[string]string) bool {
	index := flattenMetricIndex(name, tags)
	counter := s.Counters()[index]
	notFound := fmt.Sprintf("not found: key=%s, actual=%v", index, counterKeys(s.Counters()))
	if !assert.NotNil(t, counter, notFound) {
		return false
	}
	mismatch := fmt.Sprintf("current values: %v", CounterMap(s.Counters()))
	return assert.Equal(t, expected, counter.Value(), mismatch)
}

// AssertGaugeValue asserts that the given gauge has the expected value.
func AssertGaugeValue(t *testing.T, expected float64, s tally.Snapshot, name string, tags map[string]string) bool {
	index := flattenMetricIndex(name, tags)
	gauge := s.Gauges()[index]
	notFound := fmt.Sprintf("not found: key=%s, actual=%v", index, gaugeKeys(s.Gauges()))
	if !assert.NotNil(t, gauge, notFound) {
		return false
	}
	mismatch := fmt.Sprintf("current values: %v", GaugeMap(s.Gauges()))
	return assert.InDelta(t, expected, gauge.Value(), 0.0001, mismatch)
}

// AssertGaugeNil asserts that the given gauge does not exist.
func AssertGaugeNil(t *testing.T, s tally.Snapshot, name string, tags map[string]string) bool {
	index := flattenMetricIndex(name, tags)
	gauge := s.Gauges()[index]
	found := fmt.Sprintf("found: key=%s, actual=%v", index, gaugeKeys(s.Gauges()))
	return assert.Nil(t, gauge, found)
}

func flattenMetricIndex(name string, tags map[string]string) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	index := name + "+"
	for i, k := range keys {
		sep := ""
		if i != 0 {
			sep = ","
		}

		index += fmt.Sprintf("%s%s=%s", sep, k, tags[k])
	}

	return index
}

// CounterMap returns the counter map for a snapshot.
func CounterMap(m map[string]tally.CounterSnapshot) map[string]int64 {
	result := make(map[string]int64, len(m))
	for k, v := range m {
		result[k] = v.Value()
	}
	return result
}

// GaugeMap returns the gauge map for a snapshot.
func GaugeMap(m map[string]tally.GaugeSnapshot) map[string]float64 {
	result := make(map[string]float64, len(m))
	for k, v := range m {
		result[k] = v.Value()
	}
	return result
}

func counterKeys(m map[string]tally.CounterSnapshot) []string {
	r := make([]string, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}

func gaugeKeys(m map[string]tally.GaugeSnapshot) []string {
	r := make([]string, 0, len(m))
	for k := range m {
		r = append(r, k)
	}
	return r
}
