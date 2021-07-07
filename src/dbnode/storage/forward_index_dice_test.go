// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/retention"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func optionsWithIndexValues(
	indexProb float64,
	indexThreshold float64,
	retOpts retention.Options,
) Options {
	opts := DefaultTestOptions()

	idxOpts := opts.IndexOptions()
	idxOpts = idxOpts.
		SetForwardIndexProbability(indexProb).
		SetForwardIndexThreshold(indexThreshold)

	opts = DefaultTestOptions().
		SetIndexOptions(idxOpts)

	if retOpts != nil {
		seriesOpts := opts.SeriesOptions().
			SetRetentionOptions(retOpts)
		return opts.SetSeriesOptions(seriesOpts)
	}

	return opts
}

func TestDisabledForwardIndexDice(t *testing.T) {
	opts := optionsWithIndexValues(0, 0, nil)
	dice, err := newForwardIndexDice(opts)
	require.NoError(t, err)
	require.False(t, dice.enabled)

	start := xtime.Now().Truncate(time.Hour)
	end := start.Add(time.Hour)

	for ts := start; ts.Before(end); ts = ts.Add(time.Second) {
		require.False(t, dice.roll(ts))
	}
}

func TestInvalidForwardIndexDice(t *testing.T) {
	// Index probability < 0 and > 1 cases.
	invalidIndexProbabilities := []float64{-10, 10}
	for _, prob := range invalidIndexProbabilities {
		opts := optionsWithIndexValues(prob, 0, nil)
		_, err := newForwardIndexDice(opts)
		require.Error(t, err)
	}

	// Index threshold < 0 and > 1 cases.
	invalidIndexThresholds := []float64{-10, 10}
	for _, threshold := range invalidIndexThresholds {
		opts := optionsWithIndexValues(0.5, threshold, nil)
		_, err := newForwardIndexDice(opts)
		require.Error(t, err)
	}
}

func TestAlwaysOnForwardIndexDice(t *testing.T) {
	retOpts := retention.NewOptions().
		SetBlockSize(time.Hour).
		SetBufferFuture(time.Minute * 10)
	opts := optionsWithIndexValues(1, 0.9, retOpts)

	dice, err := newForwardIndexDice(opts)
	require.NoError(t, err)
	require.True(t, dice.enabled)

	var (
		start     = xtime.Now().Truncate(time.Hour)
		threshold = start.Add(time.Minute * 51)
		end       = start.Add(time.Hour)
	)

	for ts := start; ts.Before(end); ts = ts.Add(time.Second) {
		indexing := dice.roll(ts)
		if ts.Before(threshold) {
			require.False(t, indexing)
		} else {
			require.True(t, indexing)
		}
	}
}

type trackingDice struct {
	rolled  int
	success int
}

func (d *trackingDice) Rate() float64 { return 0 }
func (d *trackingDice) Roll() bool {
	d.rolled++
	return d.rolled%d.success == 0
}

func TestCustomDice(t *testing.T) {
	d := &trackingDice{
		success: 10,
	}

	dice := forwardIndexDice{
		enabled:   true,
		blockSize: time.Hour,

		forwardIndexThreshold: time.Minute * 50,
		forwardIndexDice:      d,
	}

	var (
		start     = xtime.Now().Truncate(time.Hour)
		threshold = start.Add(time.Minute * 50)
		end       = start.Add(time.Hour)
	)

	sample := 0
	for ts := start; ts.Before(end); ts = ts.Add(time.Second) {
		indexing := dice.roll(ts)

		if ts.Before(threshold) {
			require.False(t, indexing)
		} else {
			sample++
			require.Equal(t, sample%10 == 0, indexing)
		}
	}

	require.Equal(t, 600, d.rolled)
}
