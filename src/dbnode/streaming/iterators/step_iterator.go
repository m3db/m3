package iterators

import (
	"github.com/m3db/m3/src/dbnode/streaming/downsamplers"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

var _ BaseIterator = (*stepIterator)(nil)

type stepIterator struct {
	BaseIterator
	start          xtime.UnixNano
	currentStepEnd xtime.UnixNano
	step           time.Duration
	downsampler    downsamplers.Downsampler
	previous       iteratorSample
	current        iteratorSample
}

func NewStepIterator(
	underlying BaseIterator,
	start xtime.UnixNano,
	step time.Duration,
	downsampler downsamplers.Downsampler,
) BaseIterator {
	return &stepIterator{
		BaseIterator: underlying,
		start:        start,
		step:         step,
		downsampler:  downsampler,
	}
}

func (it *stepIterator) Next() bool {
	for it.BaseIterator.Next() {
		it.previous = it.current
		if it.previous.dp.TimestampNanos > 0 {
			it.downsampler.Accept(it.previous.dp.Value)
		}
		// FIXME: Users should not hold on to the returned Annotation object as it may get invalidated when the iterator calls Next().
		it.current.dp, it.current.unit, it.current.annotation = it.BaseIterator.Current()
		currentTimestamp := it.current.dp.TimestampNanos
		stepNanos := xtime.UnixNano(it.step)
		if it.currentStepEnd == 0 {
			it.currentStepEnd = it.start + ((currentTimestamp-it.start)/stepNanos+1)*stepNanos
			continue
		}
		if currentTimestamp >= it.currentStepEnd {
			it.currentStepEnd = it.start + ((currentTimestamp-it.start)/stepNanos+1)*stepNanos
			it.previous.dp.Value = it.downsampler.Emit()
			return true
		}
	}
	// BaseIterator already exhausted by here.
	if it.current.dp.TimestampNanos > 0 {
		// Always return the last value (unless BaseIterator was empty).
		it.previous = it.current
		it.downsampler.Accept(it.previous.dp.Value)
		it.previous.dp.Value = it.downsampler.Emit()
		it.current.dp.TimestampNanos = 0
		return true
	}
	return false
}

func (it *stepIterator) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return it.previous.dp, it.previous.unit, it.previous.annotation
}
