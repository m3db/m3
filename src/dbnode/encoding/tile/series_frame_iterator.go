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
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/dbnode/persist/fs"
	xtime "github.com/m3db/m3/src/x/time"
)

type seriesFrameIter struct {
	err error

	exhausted bool
	started   bool
	curr      SeriesBlockFrame

	iter fs.CrossBlockIterator

	frameStep  time.Duration
	frameStart xtime.UnixNano
}

func newSeriesFrameIterator(recorder *recorder) SeriesFrameIterator {
	return &seriesFrameIter{
		err:  errors.New("unset"),
		curr: newSeriesBlockFrame(recorder),
	}
}

func (b *seriesFrameIter) Reset(
	start xtime.UnixNano,
	frameStep time.Duration,
	iter fs.CrossBlockIterator,
) error {
	if frameStep <= 0 {
		b.err = fmt.Errorf("frame step must be > 0, is %d", frameStep)
		return b.err
	}

	b.err = nil
	b.iter = iter
	b.exhausted = false
	b.started = false
	b.frameStart = start
	b.frameStep = frameStep
	b.curr.reset(start, start+xtime.UnixNano(frameStep))

	return nil
}

func (b *seriesFrameIter) Err() error {
	return b.err
}

func (b *seriesFrameIter) Close() error {
	if b.iter != nil {
		b.iter = nil
	}

	return nil
}

func (b *seriesFrameIter) Next() bool {
	if b.err != nil || b.exhausted {
		return false
	}

	if !b.started {
		b.started = true
		// NB: initialize iterator to valid value to frameStart.
		if !b.iter.Next() {
			return false
		}
	} else {
		b.curr.reset(b.frameStart, b.frameStart+xtime.UnixNano(b.frameStep))
	}

	cutover := b.frameStart + xtime.UnixNano(b.frameStep)
	b.curr.FrameStartInclusive = b.frameStart
	b.curr.FrameEndExclusive = cutover
	b.frameStart = cutover
	firstPoint, firstUnit, firstAnnotation := b.iter.Current()
	if firstPoint.TimestampNanos >= cutover {
		// NB: empty block.
		return true
	}

	var hasAny, hasMore bool
	b.curr.record(firstPoint, firstUnit, firstAnnotation)
	for b.iter.Next() {
		hasAny = true
		dp, unit, annotation := b.iter.Current()
		if dp.TimestampNanos >= cutover {
			hasMore = true
			break
		}

		b.curr.record(dp, unit, annotation)
	}

	if !hasAny {
		b.exhausted = true
		return true
	}

	if err := b.iter.Err(); err != nil {
		b.err = err
		return false
	}

	if !hasMore {
		b.exhausted = true
	}

	return true
}

func (b *seriesFrameIter) Current() SeriesBlockFrame {
	return b.curr
}
