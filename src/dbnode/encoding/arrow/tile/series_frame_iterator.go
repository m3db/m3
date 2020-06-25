package tile

import (
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/encoding/arrow/base"
)

// SeriesFrameIterator is a frame-wise iterator across a series block.
type SeriesFrameIterator interface {
	Next() bool
	Close() error
	Current() SeriesBlockFrame
	Reset(frameStart xtime.UnixNano, iter base.BlockIterator)
}

type seriesFrameIter struct {
	exhausted bool
	started   bool
	curr      SeriesBlockFrame

	recorder *datapointRecorder
	iter     base.BlockIterator

	frameStep  xtime.UnixNano
	frameStart xtime.UnixNano
}

func newSeriesFrameIterator(
	start xtime.UnixNano,
	frameStep xtime.UnixNano,
	recorder *datapointRecorder,
	iter base.BlockIterator,
) SeriesFrameIterator {
	return &seriesFrameIter{
		recorder: recorder,
		iter:     iter,

		curr: SeriesBlockFrame{
			FrameStart: start,
			FrameEnd:   start + frameStep,
			record:     newDatapointRecord(),
		},

		frameStep:  frameStep,
		frameStart: start,
	}
}

func (b *seriesFrameIter) Reset(start xtime.UnixNano, iter base.BlockIterator) {
	if b.iter != nil {
		b.iter.Close()
	}

	b.curr.release()
	b.iter = iter
	b.frameStart = start
	b.exhausted = false
	b.started = false
}

func (b *seriesFrameIter) Close() error {
	if b.iter != nil {
		b.iter.Close()
	}

	return nil
}

func (b *seriesFrameIter) Release() {
	b.curr.release()
}

func (b *seriesFrameIter) Next() bool {
	b.curr.release()

	if !b.started {
		b.started = true
		// NB: initialize iterator to valid value to frameStart.
		if !b.iter.Next() {
			return false
		}
	}

	if b.exhausted {
		return false
	}

	cutover := b.frameStart + b.frameStep
	b.curr.FrameStart = b.frameStart
	b.curr.FrameEnd = cutover
	b.frameStart = cutover
	firstPoint := b.iter.Current()
	if firstPoint.Timestamp >= cutover {
		// NB: empty block.
		return true
	}

	var hasMore bool
	b.recorder.appendPoints(firstPoint)
	for b.iter.Next() {
		curr := b.iter.Current()
		if curr.Timestamp >= cutover {
			hasMore = true
			break
		}

		b.recorder.appendPoints(curr)
	}

	b.recorder.updateRecord(b.curr.record)
	if !hasMore {
		b.exhausted = true
	}

	return true
}

func (b *seriesFrameIter) Current() SeriesBlockFrame {
	return b.curr
}
