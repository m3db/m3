// Copyright (c) 2016 Uber Technologies, Inc.
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

package bootstrap

import (
	"testing"
	"time"

	"github.com/m3db/m3db/storage/block"

	"github.com/stretchr/testify/require"
)

func getTestResultOptions() Options {
	return NewOptions()
}

func TestResultIsEmpty(t *testing.T) {
	opts := getTestResultOptions()
	sr := NewShardResult(opts)
	require.True(t, sr.IsEmpty())
	block := opts.GetDatabaseBlockOptions().GetDatabaseBlockPool().Get()
	block.Reset(time.Now(), nil)
	sr.AddBlock("foo", block)
	require.False(t, sr.IsEmpty())
}

func TestAddBlockToResult(t *testing.T) {
	opts := getTestResultOptions()
	sr := NewShardResult(opts)
	start := time.Now()
	inputs := []struct {
		id        string
		timestamp time.Time
	}{
		{"foo", start},
		{"foo", start.Add(2 * time.Hour)},
		{"bar", start},
	}
	for _, input := range inputs {
		block := opts.GetDatabaseBlockOptions().GetDatabaseBlockPool().Get()
		block.Reset(input.timestamp, nil)
		sr.AddBlock(input.id, block)
	}
	allSeries := sr.AllSeries()
	require.Len(t, allSeries, 2)
	require.Equal(t, 2, allSeries["foo"].Len())
	require.Equal(t, 1, allSeries["bar"].Len())
}

func TestAddSeriesToResult(t *testing.T) {
	opts := getTestResultOptions()
	sr := NewShardResult(opts)
	start := time.Now()
	inputs := []struct {
		id     string
		series block.DatabaseSeriesBlocks
	}{
		{"foo", block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions())},
		{"bar", block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions())},
	}
	for _, input := range inputs {
		sr.AddSeries(input.id, input.series)
	}
	moreSeries := block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions())
	block := opts.GetDatabaseBlockOptions().GetDatabaseBlockPool().Get()
	block.Reset(start, nil)
	moreSeries.AddBlock(block)
	sr.AddSeries("foo", moreSeries)
	allSeries := sr.AllSeries()
	require.Len(t, allSeries, 2)
	require.Equal(t, 1, allSeries["foo"].Len())
	require.Equal(t, 0, allSeries["bar"].Len())
}

func TestAddResultToResult(t *testing.T) {
	opts := getTestResultOptions()
	sr := NewShardResult(opts)
	sr.AddResult(nil)
	require.True(t, sr.IsEmpty())
	other := NewShardResult(opts)
	other.AddSeries("foo", block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions()))
	other.AddSeries("bar", block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions()))
	sr.AddResult(other)
	require.Len(t, sr.AllSeries(), 2)
}

func TestRemoveSeriesFromResult(t *testing.T) {
	opts := getTestResultOptions()
	sr := NewShardResult(opts)
	inputs := []struct {
		id     string
		series block.DatabaseSeriesBlocks
	}{
		{"foo", block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions())},
		{"bar", block.NewDatabaseSeriesBlocks(opts.GetDatabaseBlockOptions())},
	}
	for _, input := range inputs {
		sr.AddSeries(input.id, input.series)
	}
	require.Equal(t, 2, len(sr.AllSeries()))
	sr.RemoveSeries("bar")
	require.Equal(t, 1, len(sr.AllSeries()))
	sr.RemoveSeries("nonexistent")
	require.Equal(t, 1, len(sr.AllSeries()))
}
