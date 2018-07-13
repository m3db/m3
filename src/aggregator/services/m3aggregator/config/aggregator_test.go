// Copyright (c) 2017 Uber Technologies, Inc.
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

package config

import (
	"testing"
	"time"

	"github.com/m3db/m3aggregator/aggregator"

	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestJitterBuckets(t *testing.T) {
	config := `
    - flushInterval: 1m
      maxJitterPercent: 1.0
    - flushInterval: 10m
      maxJitterPercent: 0.5
    - flushInterval: 1h
      maxJitterPercent: 0.25`

	var buckets jitterBuckets
	require.NoError(t, yaml.Unmarshal([]byte(config), &buckets))

	maxJitterFn, err := buckets.NewMaxJitterFn()
	require.NoError(t, err)

	inputs := []struct {
		interval          time.Duration
		expectedMaxJitter time.Duration
	}{
		{interval: time.Second, expectedMaxJitter: time.Second},
		{interval: 10 * time.Second, expectedMaxJitter: 10 * time.Second},
		{interval: time.Minute, expectedMaxJitter: time.Minute},
		{interval: 10 * time.Minute, expectedMaxJitter: 5 * time.Minute},
		{interval: time.Hour, expectedMaxJitter: 15 * time.Minute},
		{interval: 6 * time.Hour, expectedMaxJitter: 90 * time.Minute},
	}
	for _, input := range inputs {
		require.Equal(t, input.expectedMaxJitter, maxJitterFn(input.interval))
	}
}

func TestMaxAllowedForwardingDelayFnJitterEnabled(t *testing.T) {
	maxJitterFn := func(resolution time.Duration) time.Duration {
		if resolution <= time.Second {
			return time.Second
		}
		if resolution <= time.Minute {
			return time.Duration(0.5 * float64(resolution))
		}
		return time.Duration(0.25 * float64(resolution))
	}
	c := forwardingConfiguration{MaxSingleDelay: 5 * time.Second}
	fn := c.MaxAllowedForwardingDelayFn(true, maxJitterFn)

	inputs := []struct {
		resolution        time.Duration
		numForwardedTimes int
		expected          time.Duration
	}{
		{resolution: time.Second, numForwardedTimes: 1, expected: 6 * time.Second},
		{resolution: time.Second, numForwardedTimes: 3, expected: 16 * time.Second},
		{resolution: 10 * time.Second, numForwardedTimes: 1, expected: 10 * time.Second},
		{resolution: 10 * time.Second, numForwardedTimes: 3, expected: 20 * time.Second},
		{resolution: 10 * time.Minute, numForwardedTimes: 1, expected: 155 * time.Second},
		{resolution: 10 * time.Minute, numForwardedTimes: 3, expected: 165 * time.Second},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, fn(input.resolution, input.numForwardedTimes))
	}
}

func TestMaxAllowedForwardingDelayFnJitterDisabled(t *testing.T) {
	maxJitterFn := func(resolution time.Duration) time.Duration {
		if resolution <= time.Second {
			return time.Second
		}
		if resolution <= time.Minute {
			return time.Duration(0.5 * float64(resolution))
		}
		return time.Duration(0.25 * float64(resolution))
	}
	c := forwardingConfiguration{MaxSingleDelay: 5 * time.Second}
	fn := c.MaxAllowedForwardingDelayFn(false, maxJitterFn)

	inputs := []struct {
		resolution        time.Duration
		numForwardedTimes int
		expected          time.Duration
	}{
		{resolution: time.Second, numForwardedTimes: 1, expected: 6 * time.Second},
		{resolution: time.Second, numForwardedTimes: 3, expected: 16 * time.Second},
		{resolution: 10 * time.Second, numForwardedTimes: 1, expected: 15 * time.Second},
		{resolution: 10 * time.Second, numForwardedTimes: 3, expected: 25 * time.Second},
		{resolution: 10 * time.Minute, numForwardedTimes: 1, expected: 605 * time.Second},
		{resolution: 10 * time.Minute, numForwardedTimes: 3, expected: 615 * time.Second},
	}
	for _, input := range inputs {
		require.Equal(t, input.expected, fn(input.resolution, input.numForwardedTimes))
	}
}

func TestFlushIntervalFn(t *testing.T) {
	overrides := map[time.Duration]time.Duration{
		10 * time.Second: 5 * time.Second,
		time.Minute:      20 * time.Second,
	}
	c := forwardingConfiguration{FlushIntervalOverrides: overrides}
	flushIntervalFn := c.FlushIntervalFn()
	inputs := []struct {
		metricType aggregator.IncomingMetricType
		resolution time.Duration
		expected   time.Duration
	}{
		{
			metricType: aggregator.StandardIncomingMetric,
			resolution: 10 * time.Second,
			expected:   10 * time.Second,
		},
		{
			metricType: aggregator.ForwardedIncomingMetric,
			resolution: 10 * time.Second,
			expected:   5 * time.Second,
		},
		{
			metricType: aggregator.ForwardedIncomingMetric,
			resolution: time.Minute,
			expected:   20 * time.Second,
		},
		{
			metricType: aggregator.ForwardedIncomingMetric,
			resolution: 10 * time.Minute,
			expected:   10 * time.Minute,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, flushIntervalFn(input.metricType, input.resolution))
	}
}

func TestForwardingSourcesTTLFn(t *testing.T) {
	c := forwardingSourcesTTLConfiguration{
		ByResolution: 10,
		ByDuration:   5 * time.Minute,
	}
	ttlFn := c.ForwardingSourcesTTLFn()
	inputs := []struct {
		resolution time.Duration
		expected   time.Duration
	}{
		{
			resolution: 10 * time.Second,
			expected:   5 * time.Minute,
		},
		{
			resolution: time.Minute,
			expected:   10 * time.Minute,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, ttlFn(input.resolution))
	}
}
