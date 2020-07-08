package tile

import (
	"errors"
	"fmt"

	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/encoding"
)

// SeriesFrameIterator is a frame-wise iterator across a series block.
type SeriesFrameIterator interface {
	// Err returns any errors encountered.
	Err() error
	// Next moves to the next element.
	Next() bool
	// Close closes the iterator.
	Close() error
	// Current returns the current series block frame.
	Current() SeriesBlockFrame
	// Reset resets the series frame iterator.
	Reset(
		start xtime.UnixNano,
		step xtime.UnixNano,
		it encoding.ReaderIterator,
	) error
}

type seriesFrameIter struct {
	err error

	exhausted bool
	started   bool
	curr      SeriesBlockFrame

	recorder *datapointRecorder
	iter     encoding.ReaderIterator

	frameStep  xtime.UnixNano
	frameStart xtime.UnixNano
}

func newSeriesFrameIterator(recorder *datapointRecorder) SeriesFrameIterator {
	return &seriesFrameIter{
		recorder: recorder,
		err:      errors.New("unset"),
		curr: SeriesBlockFrame{
			record: newDatapointRecord(),
		},
	}
}

func (b *seriesFrameIter) Reset(
	start xtime.UnixNano,
	frameStep xtime.UnixNano,
	iter encoding.ReaderIterator,
) error {
	if b.iter != nil {
		b.iter.Close()
		b.iter = nil
	}

	if frameStep <= 0 {
		b.err = fmt.Errorf("frame step must be >= 0, is %d", frameStep)
		return b.err
	}

	b.err = nil
	b.iter = iter
	b.started = false
	b.exhausted = false
	b.frameStart = start
	b.frameStep = frameStep
	b.curr.reset(start, start+frameStep)
	return nil
}

func (b *seriesFrameIter) Err() error {
	return b.err
}

func (b *seriesFrameIter) Close() error {
	if b.iter != nil {
		b.iter.Close()
		b.iter = nil
	}

	return nil
}

func (b *seriesFrameIter) Release() {
	b.curr.release()
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
		b.curr.release()
	}

	cutover := b.frameStart + b.frameStep
	b.curr.FrameStart = b.frameStart
	b.curr.FrameEnd = cutover
	b.frameStart = cutover
	firstPoint, _, _ := b.iter.Current()
	if firstPoint.TimestampNanos >= cutover {
		// NB: empty block.
		b.recorder.updateRecord(b.curr.record)
		return true
	}

	var hasAny bool
	var hasMore bool
	b.recorder.appendPoints(firstPoint)
	for b.iter.Next() {
		hasAny = true
		curr, _, _ := b.iter.Current()
		if curr.TimestampNanos >= cutover {
			hasMore = true
			break
		}

		b.recorder.appendPoints(curr)
	}

	b.recorder.updateRecord(b.curr.record)

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
