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

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/storage"

	"github.com/stretchr/testify/require"
)

func getTestResultOptions() m3db.DatabaseOptions {
	return storage.NewDatabaseOptions()
}

func TestResultIsEmpty(t *testing.T) {
	opts := getTestResultOptions()
	sr := NewShardResult(opts)
	require.True(t, sr.IsEmpty())
	sr.AddBlock("foo", storage.NewDatabaseBlock(time.Now(), nil, nil))
	require.False(t, sr.IsEmpty())
}

func TestAddBlockToResult(t *testing.T) {
	opts := getTestResultOptions()
	sr := NewShardResult(opts)
	start := time.Now()
	inputs := []struct {
		id    string
		block m3db.DatabaseBlock
	}{
		{"foo", storage.NewDatabaseBlock(start, nil, nil)},
		{"foo", storage.NewDatabaseBlock(start.Add(2*time.Hour), nil, nil)},
		{"bar", storage.NewDatabaseBlock(start, nil, nil)},
	}
	for _, input := range inputs {
		sr.AddBlock(input.id, input.block)
	}
	allSeries := sr.GetAllSeries()
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
		series m3db.DatabaseSeriesBlocks
	}{
		{"foo", storage.NewDatabaseSeriesBlocks(opts)},
		{"bar", storage.NewDatabaseSeriesBlocks(opts)},
	}
	for _, input := range inputs {
		sr.AddSeries(input.id, input.series)
	}
	moreSeries := storage.NewDatabaseSeriesBlocks(opts)
	moreSeries.AddBlock(storage.NewDatabaseBlock(start, nil, nil))
	sr.AddSeries("foo", moreSeries)
	allSeries := sr.GetAllSeries()
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
	other.AddSeries("foo", storage.NewDatabaseSeriesBlocks(opts))
	other.AddSeries("bar", storage.NewDatabaseSeriesBlocks(opts))
	sr.AddResult(other)
	require.Len(t, sr.GetAllSeries(), 2)
}
