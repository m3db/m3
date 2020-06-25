package tile

import (
	"github.com/m3db/m3/src/dbnode/encoding/arrow/base"
	xtime "github.com/m3db/m3/src/x/time"
)

type resultsIterator interface {
	Next() bool
	Close() error
	Current() SeriesBlockIterator
}

type resultsIter struct {
	recorder *datapointRecorder
	iter     base.MultiSeriesIterator

	exhausted bool
	step      xtime.UnixNano
	start     xtime.UnixNano

	curr SeriesBlockIterator
}

func newResultsIterator(
	start xtime.UnixNano,
	step xtime.UnixNano,
	recorder *datapointRecorder,
	iter base.MultiSeriesIterator,
) resultsIterator {
	return &resultsIter{
		curr:     newSeriesBlockIterator(start, step, recorder, nil),
		start:    start,
		step:     step,
		recorder: recorder,
		iter:     iter,
	}
}

func (b *resultsIter) Close() error {
	b.recorder.release()
	return nil
}

func (b *resultsIter) Next() bool {
	if b.iter.Next() {
		iter := b.iter.Current()
		b.curr.Reset(iter)
		return true
	}

	b.curr.Close()
	b.curr = nil
	return false
}

func (b *resultsIter) Current() SeriesBlockIterator {
	return b.curr
}
