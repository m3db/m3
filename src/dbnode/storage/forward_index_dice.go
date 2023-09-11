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
	"fmt"
	"time"

	xtime "github.com/m3db/m3/src/x/time"
)

// forwardIndexDice is a die roll that adds a chance for incoming index writes
// arriving near a block boundary to be duplicated and written to the next block
// index, adding jitter and smoothing index load so that block boundaries do not
// cause a huge influx of new documents that all need to be indexed at once.
type forwardIndexDice struct {
	enabled   bool
	blockSize time.Duration

	forwardIndexThreshold time.Duration
	forwardIndexDice      dice
}

func newForwardIndexDice(
	opts Options,
) (forwardIndexDice, error) {
	var (
		indexOpts  = opts.IndexOptions()
		seriesOpts = opts.SeriesOptions()

		probability = indexOpts.ForwardIndexProbability()
	)

	// NB: if not enabled, return a no-op forward index dice.
	if probability == 0 {
		return forwardIndexDice{}, nil
	}

	var (
		threshold = indexOpts.ForwardIndexThreshold()

		retention    = seriesOpts.RetentionOptions()
		bufferFuture = retention.BufferFuture()
		blockSize    = retention.BlockSize()

		forwardIndexThreshold time.Duration
	)

	if threshold < 0 || threshold > 1 {
		return forwardIndexDice{},
			fmt.Errorf("invalid forward write threshold %f", threshold)
	}

	bufferFragment := float64(bufferFuture) * threshold
	forwardIndexThreshold = blockSize - time.Duration(bufferFragment)

	dice, err := newDice(probability)
	if err != nil {
		return forwardIndexDice{},
			fmt.Errorf("cannot create forward write dice: %s", err)
	}

	return forwardIndexDice{
		enabled:   true,
		blockSize: blockSize,

		forwardIndexThreshold: forwardIndexThreshold,
		forwardIndexDice:      dice,
	}, nil
}

// roll decides if a timestamp is eligible for forward index writes.
func (o *forwardIndexDice) roll(timestamp xtime.UnixNano) bool {
	if !o.enabled {
		return false
	}

	threshold := timestamp.Truncate(o.blockSize).Add(o.forwardIndexThreshold)
	if !timestamp.Before(threshold) {
		return o.forwardIndexDice.Roll()
	}

	return false
}
