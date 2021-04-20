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

package aggregator

import (
	"math"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"

	"github.com/stretchr/testify/require"
)

var (
	testShard = uint32(0)
)

func TestAggregatorShardCutoffNanos(t *testing.T) {
	shard := newAggregatorShard(testShard, newTestOptions())
	inputs := []int64{0, 12345, math.MaxInt64}
	for _, input := range inputs {
		shard.cutoffNanos = input
		require.Equal(t, input, shard.CutoffNanos())
	}
}

func TestAggregatorShardIsWriteable(t *testing.T) {
	now := time.Unix(0, 12345)
	shard := newAggregatorShard(testShard, newTestOptions())
	shard.nowFn = func() time.Time { return now }

	inputs := []struct {
		earliestNanos      int64
		latestNanos        int64
		expectedIsWritable bool
	}{
		{earliestNanos: 0, latestNanos: math.MaxInt64, expectedIsWritable: true},
		{earliestNanos: 12345, latestNanos: 67890, expectedIsWritable: true},
		{earliestNanos: 20000, latestNanos: math.MaxInt64, expectedIsWritable: false},
		{earliestNanos: 0, latestNanos: 12345, expectedIsWritable: false},
	}
	for _, input := range inputs {
		shard.earliestWritableNanos = input.earliestNanos
		shard.latestWriteableNanos = input.latestNanos
		require.Equal(t, input.expectedIsWritable, shard.IsWritable())
	}
}

func TestAggregatorShardIsCutoff(t *testing.T) {
	now := time.Unix(0, 12345)
	shard := newAggregatorShard(testShard, newTestOptions())
	shard.nowFn = func() time.Time { return now }

	inputs := []struct {
		cutoffNanos int64
		expected    bool
	}{
		{cutoffNanos: 0, expected: true},
		{cutoffNanos: 12345, expected: true},
		{cutoffNanos: 12346, expected: false},
		{cutoffNanos: math.MaxInt64, expected: false},
	}
	for _, input := range inputs {
		shard.cutoffNanos = input.cutoffNanos
		require.Equal(t, input.expected, shard.IsCutoff())
	}
}

func TestAggregatorShardSetWritableRange(t *testing.T) {
	testNanos := int64(1234)
	opts := newTestOptions().
		SetEntryCheckInterval(0).
		SetBufferDurationBeforeShardCutover(time.Duration(500)).
		SetBufferDurationAfterShardCutoff(time.Duration(1000))
	shard := newAggregatorShard(testShard, opts)

	inputs := []struct {
		timeRange             timeRange
		expectedEarliestNanos int64
		expectedLatestNanos   int64
	}{
		{
			timeRange:             timeRange{cutoverNanos: 0, cutoffNanos: int64(math.MaxInt64)},
			expectedEarliestNanos: 0,
			expectedLatestNanos:   int64(math.MaxInt64),
		},
		{
			timeRange:             timeRange{cutoverNanos: testNanos, cutoffNanos: int64(math.MaxInt64)},
			expectedEarliestNanos: testNanos - 500,
			expectedLatestNanos:   int64(math.MaxInt64),
		},
		{
			timeRange:             timeRange{cutoverNanos: 0, cutoffNanos: testNanos},
			expectedEarliestNanos: 0,
			expectedLatestNanos:   testNanos + 1000,
		},
	}
	for _, input := range inputs {
		shard.SetWriteableRange(input.timeRange)
		require.Equal(t, input.expectedEarliestNanos, shard.earliestWritableNanos)
		require.Equal(t, input.expectedLatestNanos, shard.latestWriteableNanos)
	}
}

func TestAggregatorShardAddUntimedShardClosed(t *testing.T) {
	shard := newAggregatorShard(testShard, newTestOptions().SetEntryCheckInterval(0))
	shard.closed = true
	err := shard.AddUntimed(testUntimedMetric, testStagedMetadatas)
	require.Equal(t, errAggregatorShardClosed, err)
}

func TestAggregatorShardAddUntimedShardNotWriteable(t *testing.T) {
	now := time.Unix(0, 12345)
	shard := newAggregatorShard(testShard, newTestOptions())
	shard.nowFn = func() time.Time { return now }

	inputs := []struct {
		earliestNanos int64
		latestNanos   int64
	}{
		{earliestNanos: 23456, latestNanos: 34567},
		{earliestNanos: 1234, latestNanos: 3456},
		{earliestNanos: 0, latestNanos: 0},
		{earliestNanos: math.MaxInt64, latestNanos: math.MaxInt64},
	}
	for _, input := range inputs {
		shard.earliestWritableNanos = input.earliestNanos
		shard.latestWriteableNanos = input.latestNanos
		err := shard.AddUntimed(testUntimedMetric, testStagedMetadatas)
		require.Equal(t, errAggregatorShardNotWriteable, err)
	}
}

func TestAggregatorShardAddUntimedSuccess(t *testing.T) {
	shard := newAggregatorShard(testShard, newTestOptions())
	require.Equal(t, testShard, shard.ID())

	var (
		resultMu        unaggregated.MetricUnion
		resultMetadatas metadata.StagedMetadatas
	)
	shard.addUntimedFn = func(
		mu unaggregated.MetricUnion,
		sm metadata.StagedMetadatas,
	) error {
		resultMu = mu
		resultMetadatas = sm
		return nil
	}

	shard.SetWriteableRange(timeRange{cutoverNanos: 0, cutoffNanos: math.MaxInt64})
	require.NoError(t, shard.AddUntimed(testUntimedMetric, testStagedMetadatas))
	require.Equal(t, testUntimedMetric, resultMu)
	require.Equal(t, testStagedMetadatas, resultMetadatas)
}

func TestAggregatorShardAddTimedShardNotWriteable(t *testing.T) {
	now := time.Unix(0, 12345)
	shard := newAggregatorShard(testShard, newTestOptions())
	shard.nowFn = func() time.Time { return now }

	inputs := []struct {
		earliestNanos int64
		latestNanos   int64
	}{
		{earliestNanos: 23456, latestNanos: 34567},
		{earliestNanos: 1234, latestNanos: 3456},
		{earliestNanos: 0, latestNanos: 0},
		{earliestNanos: math.MaxInt64, latestNanos: math.MaxInt64},
	}
	for _, input := range inputs {
		shard.earliestWritableNanos = input.earliestNanos
		shard.latestWriteableNanos = input.latestNanos
		err := shard.AddTimed(testTimedMetric, testTimedMetadata)
		require.Equal(t, errAggregatorShardNotWriteable, err)
	}
}

func TestAggregatorShardAddTimedSuccess(t *testing.T) {
	shard := newAggregatorShard(testShard, newTestOptions())
	require.Equal(t, testShard, shard.ID())

	var (
		resultMetric   aggregated.Metric
		resultMetadata metadata.TimedMetadata
	)
	shard.addTimedFn = func(
		metric aggregated.Metric,
		metadata metadata.TimedMetadata,
	) error {
		resultMetric = metric
		resultMetadata = metadata
		return nil
	}

	shard.SetWriteableRange(timeRange{cutoverNanos: 0, cutoffNanos: math.MaxInt64})
	require.NoError(t, shard.AddTimed(testTimedMetric, testTimedMetadata))
	require.Equal(t, testTimedMetric, resultMetric)
	require.Equal(t, testTimedMetadata, resultMetadata)
}

func TestAggregatorShardAddForwardedShardNotWriteable(t *testing.T) {
	now := time.Unix(0, 12345)
	shard := newAggregatorShard(testShard, newTestOptions())
	shard.nowFn = func() time.Time { return now }

	inputs := []struct {
		earliestNanos int64
		latestNanos   int64
	}{
		{earliestNanos: 23456, latestNanos: 34567},
		{earliestNanos: 1234, latestNanos: 3456},
		{earliestNanos: 0, latestNanos: 0},
		{earliestNanos: math.MaxInt64, latestNanos: math.MaxInt64},
	}
	for _, input := range inputs {
		shard.earliestWritableNanos = input.earliestNanos
		shard.latestWriteableNanos = input.latestNanos
		err := shard.AddForwarded(testForwardedMetric, testForwardMetadata)
		require.Equal(t, errAggregatorShardNotWriteable, err)
	}
}

func TestAggregatorShardAddForwardedSuccess(t *testing.T) {
	shard := newAggregatorShard(testShard, newTestOptions())
	require.Equal(t, testShard, shard.ID())

	var (
		resultMetric   aggregated.ForwardedMetric
		resultMetadata metadata.ForwardMetadata
	)
	shard.addForwardedFn = func(
		metric aggregated.ForwardedMetric,
		metadata metadata.ForwardMetadata,
	) error {
		resultMetric = metric
		resultMetadata = metadata
		return nil
	}

	shard.SetWriteableRange(timeRange{cutoverNanos: 0, cutoffNanos: math.MaxInt64})
	require.NoError(t, shard.AddForwarded(testForwardedMetric, testForwardMetadata))
	require.Equal(t, testForwardedMetric, resultMetric)
	require.Equal(t, testForwardMetadata, resultMetadata)
}

func TestAggregatorShardClose(t *testing.T) {
	shard := newAggregatorShard(testShard, newTestOptions())

	// Close the shard.
	shard.Close()

	// Assert the shard is closed.
	require.True(t, shard.closed)

	// Closing the shard again is a no op.
	shard.Close()
}
