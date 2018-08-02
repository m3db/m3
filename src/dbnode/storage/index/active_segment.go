// Copyright (c) 2018 Uber Technologies, Inc.
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

package index

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/dbnode/storage/index/segments"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
)

const (
	secondsPerMinute       = 60
	secondsPerHour         = 60 * 60
	secondsPerDay          = 24 * secondsPerHour
	unixToInternal   int64 = (1969*365 + 1969/4 - 1969/100 + 1969/400) * secondsPerDay
)

var (
	maxTime = time.Unix(1<<63-1-unixToInternal, 999999999)
)

var (
	errActiveSegmentNotSealed     = errors.New("active segment is not sealed")
	errActiveSegmentAlreadySealed = errors.New("segment already sealed")
	errActiveSegmentSealed        = errors.New("segment is sealed")
)

func newMutableActiveSegment(
	creationTime time.Time,
	seg segment.MutableSegment,
) *activeSegment {
	return &activeSegment{
		writable:       true,
		creationTime:   creationTime,
		segmentType:    segments.MutableType,
		mutableSegment: seg,
		earliestWrite:  maxTime,
	}
}

func newFSTActiveSegment(
	creationTime time.Time,
	seg segment.Segment,
	compactionNumber int,
) *activeSegment {
	return &activeSegment{
		creationTime:     creationTime,
		segmentType:      segments.FSTType,
		fstSegment:       seg,
		compactionNumber: compactionNumber,
	}
}

// activeSegment starts out backed by a mutable segment, which is rotated
// to a FST segment based on size constraints.
type activeSegment struct {
	// vars used for the write/compaction lifecycle maintenance.
	// compacting is used to track whether the segment is currently involved
	// in a compaction.
	compacting bool
	// writable indicates if the segment can still be used for writes.
	writable bool

	// the following vars are used for the read lifecycle maintenance.
	// readsWg is used to track any pending read accesors for the given segment.
	readsWg sync.WaitGroup
	// sealed indicates if the segment has been marked for removal, once it's marked
	// sealed, no new reads are allowed from the segment.
	sealed bool

	creationTime   time.Time
	segmentType    segments.Type
	mutableSegment segment.MutableSegment
	fstSegment     segment.Segment

	// some stats used for metrics
	// if the segment is backed by a mutableSegment, this value represents the earliest
	// write received. it's meaningless if the segment have been compacted
	earliestWrite time.Time
	// compactionNumber reflects the number of times the segments have been compacted.
	// It's a measure of repeated resource usage.
	compactionNumber int
}

func (a *activeSegment) MutableAndCompactable(opts compaction.PlannerOptions) bool {
	if a.segmentType != segments.MutableType {
		return false
	}
	size := a.mutableSegment.Size()
	if size >= opts.MutableSegmentSizeThreshold {
		return true
	}
	if size > 0 && time.Since(a.creationTime) > opts.MutableCompactionAgeThreshold {
		return true
	}
	return false
}

func (a *activeSegment) UpdateEarliestWrite(t time.Time) {
	if t.Before(a.earliestWrite) {
		a.earliestWrite = t
	}
}

func (a *activeSegment) Segment() segment.Segment {
	if a.segmentType == segments.FSTType {
		return a.fstSegment
	}
	return a.mutableSegment
}

func (a *activeSegment) Size() int64 {
	return a.Segment().Size()
}

func (a *activeSegment) IsSealed() bool {
	return a.sealed
}

func (a *activeSegment) Reader() (m3ninxindex.Reader, error) {
	if a.IsSealed() {
		return nil, errActiveSegmentSealed
	}
	reader, err := a.Segment().Reader()
	if err == nil {
		a.readsWg.Add(1)
	}
	return reader, err
}

func (a *activeSegment) ReaderDone() {
	a.readsWg.Done()
}

func (a *activeSegment) Seal() error {
	if a.IsSealed() {
		return errActiveSegmentAlreadySealed
	}
	a.sealed = true
	return nil
}

func (a *activeSegment) Close() error {
	if !a.IsSealed() {
		return errActiveSegmentNotSealed
	}

	// ensure there are no readers and clean up the resources async
	go func() {
		a.readsWg.Wait()
		if err := a.Segment().Close(); err != nil {
			fmt.Fprintf(os.Stderr, "bad active segement close: %v", err)
		}
		a.mutableSegment = nil
		a.fstSegment = nil
	}()
	return nil
}
