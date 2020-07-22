package tile

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/m3db/m3/src/dbnode/encoding"
)

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
	id ident.ID,
	tags ident.TagIterator,
) error {
	if frameStep <= 0 {
		b.err = fmt.Errorf("frame step must be >= 0, is %d", frameStep)
		return b.err
	}

	id.NoFinalize()
	b.err = nil
	b.iter = iter
	b.started = false
	b.exhausted = false
	b.frameStart = start
	b.frameStep = frameStep
	b.curr.reset(start, start+frameStep, id, tags)
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

	b.curr.tags.Close()
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
	firstPoint, firstUnit, firstAnnotation := b.iter.Current()
	if firstPoint.TimestampNanos >= cutover {
		// NB: empty block.
		b.recorder.updateRecord(b.curr.record)
		b.curr.tags.Rewind()
		return true
	}

	var hasAny bool
	var hasMore bool
	b.recorder.record(firstPoint, firstUnit, firstAnnotation)
	for b.iter.Next() {
		hasAny = true
		curr, unit, annotation := b.iter.Current()
		if curr.TimestampNanos >= cutover {
			hasMore = true
			break
		}

		b.recorder.record(curr, unit, annotation)
	}

	b.recorder.updateRecord(b.curr.record)
	b.curr.tags.Rewind()

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
