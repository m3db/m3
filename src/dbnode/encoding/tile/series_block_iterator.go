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

package tile

import (
	"time"

	"github.com/m3db/m3/src/dbnode/storage/wide"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

type seriesBlockIter struct {
	err       error
	exhausted bool

	blockIter wide.CrossBlockIterator
	step      time.Duration
	start     xtime.UnixNano

	frameIter SeriesFrameIterator
	metadata  wide.SeriesMetadata
}

// NewSeriesBlockIterator creates a new SeriesBlockIterator.
func NewSeriesBlockIterator(
	blockIter wide.CrossBlockIterator,
	opts Options,
) (SeriesBlockIterator, error) {
	return &seriesBlockIter{
		start:     opts.Start,
		step:      opts.FrameSize,
		blockIter: blockIter,

		frameIter: newSeriesFrameIterator(newRecorder()),
	}, nil
}

func (b *seriesBlockIter) Next() bool {
	if b.exhausted || b.err != nil {
		return false
	}

	if !b.blockIter.Next() {
		b.exhausted = true
		b.err = b.blockIter.Err()
		return false
	}

	queryIter := b.blockIter.Current()
	b.metadata = queryIter.SeriesMetadata()
	b.frameIter.Reset(b.start, b.step, queryIter)
	return true
}

func (b *seriesBlockIter) Current() (SeriesFrameIterator, ident.BytesID, ts.EncodedTags) {
	return b.frameIter, b.metadata.ID, b.metadata.EncodedTags
}

func (b *seriesBlockIter) Close() error {
	b.blockIter.Close()
	return b.frameIter.Close()
}

func (b *seriesBlockIter) Err() error {
	return b.err
}
