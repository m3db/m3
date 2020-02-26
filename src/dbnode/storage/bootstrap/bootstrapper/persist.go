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

package bootstrapper

import (
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/x/mmap"
	xtime "github.com/m3db/m3/src/x/time"
)

const (
	mmapBootstrapIndexName = "mmap.bootstrap.index"
)

// SharedPersistManager is a lockable persist manager that's safe to be shared across threads.
type SharedPersistManager struct {
	sync.Mutex
	Mgr persist.Manager
}

// SharedCompactor is a lockable compactor that's safe to be shared across threads.
type SharedCompactor struct {
	sync.Mutex
	Compactor *compaction.Compactor
}

// PersistBootstrapIndexSegment is a helper function that persists bootstrapped index segments for a ns -> block of time.
func PersistBootstrapIndexSegment(
	ns namespace.Metadata,
	requestedRanges result.ShardTimeRanges,
	indexResults result.IndexResults,
	builder segment.DocumentsBuilder,
	persistManager *SharedPersistManager,
	resultOpts result.Options,
) error {
	// If we're performing an index run with persistence enabled
	// determine if we covered a full block exactly (which should
	// occur since we always group readers by block size).
	min, max := requestedRanges.MinMax()
	blockSize := ns.Options().IndexOptions().BlockSize()
	blockStart := min.Truncate(blockSize)
	blockEnd := blockStart.Add(blockSize)
	expectedRangeStart, expectedRangeEnd := blockStart, blockEnd

	// Index blocks can be arbitrarily larger than data blocks, but the
	// retention of the namespace is based on the size of the data blocks,
	// not the index blocks. As a result, it's possible that the block start
	// for the earliest index block is before the earliest possible retention
	// time.
	// If that is the case, then we snap the expected range start to the
	// earliest retention block start because that is the point in time for
	// which we'll actually have data available to construct a segment from.
	//
	// Example:
	//  Index block size: 4 hours
	//  Data block size: 2 hours
	//  Retention: 6 hours
	//           [12PM->2PM][2PM->4PM][4PM->6PM] (Data Blocks)
	// [10AM     ->     2PM][2PM     ->     6PM] (Index Blocks)
	retentionOpts := ns.Options().RetentionOptions()
	nowFn := resultOpts.ClockOptions().NowFn()
	earliestRetentionTime := retention.FlushTimeStart(retentionOpts, nowFn())
	if blockStart.Before(earliestRetentionTime) {
		expectedRangeStart = earliestRetentionTime
	}

	shards := make(map[uint32]struct{})
	expectedRanges := make(result.ShardTimeRanges, len(requestedRanges))
	for shard := range requestedRanges {
		shards[shard] = struct{}{}
		expectedRanges[shard] = xtime.NewRanges(xtime.Range{
			Start: expectedRangeStart,
			End:   expectedRangeEnd,
		})
	}

	indexBlock, ok := indexResults[xtime.ToUnixNano(blockStart)]
	if !ok {
		// NB(bodu): We currently write empty data files to disk, which means that we can attempt to bootstrap
		// time ranges that have no data and no index block.
		// For example:
		// - peers data bootstrap from peer nodes receives peer blocks w/ no data (empty)
		// - peers data bootstrap writes empty ts data files to disk
		// - peers index bootstrap reads empty ts data files md from disk
		// - attempt to bootstrap time ranges that have no index results block
		return fmt.Errorf("could not find index block in results: time=%s, ts=%d",
			blockStart.String(), blockStart.UnixNano())
	}
	if len(builder.Docs()) == 0 {
		// No-op if there are no documents that ned to be written for this time block (nothing to persist).
		return nil
	}

	var (
		fulfilled         = indexBlock.Fulfilled()
		success           = false
		persistedSegments []segment.Segment
	)
	defer func() {
		if !success {
			return
		}

		// Combine persisted  and existing segments.
		segments := make([]segment.Segment, 0, len(persistedSegments))
		for _, pSeg := range persistedSegments {
			segments = append(segments, NewSegment(pSeg, true))
		}
		for _, seg := range indexBlock.Segments() {
			segments = append(segments, seg)
		}

		// Now replace the active segment with the persisted segment.
		newFulfilled := fulfilled.Copy()
		newFulfilled.AddRanges(expectedRanges)
		replacedBlock := result.NewIndexBlock(blockStart, segments, newFulfilled)
		indexResults[xtime.ToUnixNano(blockStart)] = replacedBlock
	}()

	// Check that we completely fulfilled all shards for the block
	// and we didn't bootstrap any more/less than expected.
	requireFulfilled := expectedRanges.Copy()
	requireFulfilled.Subtract(fulfilled)
	exactStartEnd := max.Equal(blockStart.Add(blockSize))
	if !exactStartEnd || !requireFulfilled.IsEmpty() {
		return fmt.Errorf("persistent fs index bootstrap invalid ranges to persist: "+
			"expected=%v, actual=%v, fulfilled=%v, exactStartEnd=%v, requireFulfilledEmpty=%v",
			expectedRanges.String(), requestedRanges.String(), fulfilled.String(),
			exactStartEnd, requireFulfilled.IsEmpty())
	}

	// NB(r): Need to get an exclusive lock to actually write the segment out
	// due to needing to incrementing the index file set volume index and also
	// using non-thread safe resources on the persist manager.
	persistManager.Lock()
	defer persistManager.Unlock()

	flush, err := persistManager.Mgr.StartIndexPersist()
	if err != nil {
		return err
	}

	var calledDone bool
	defer func() {
		if !calledDone {
			flush.DoneIndex()
		}
	}()

	preparedPersist, err := flush.PrepareIndex(persist.IndexPrepareOptions{
		NamespaceMetadata: ns,
		BlockStart:        indexBlock.BlockStart(),
		FileSetType:       persist.FileSetFlushType,
		Shards:            shards,
	})
	if err != nil {
		return err
	}

	var calledClose bool
	defer func() {
		if !calledClose {
			preparedPersist.Close()
		}
	}()

	if err := preparedPersist.Persist(builder); err != nil {
		return err
	}

	calledClose = true
	persistedSegments, err = preparedPersist.Close()
	if err != nil {
		return err
	}

	calledDone = true
	if err := flush.DoneIndex(); err != nil {
		return err
	}

	// Indicate the defer above should merge newly built segments w/ existing.
	success = true
	return nil
}

// BuildBootstrapIndexSegment is a helper function that builds (in memory) bootstrapped index segments for a ns -> block of time.
func BuildBootstrapIndexSegment(
	ns namespace.Metadata,
	requestedRanges result.ShardTimeRanges,
	indexResults result.IndexResults,
	builder segment.DocumentsBuilder,
	compactor *SharedCompactor,
	resultOpts result.Options,
	mmapReporter mmap.Reporter,
) error {
	// If we're performing an index run with persistence enabled
	// determine if we covered a full block exactly (which should
	// occur since we always group readers by block size).
	min, _ := requestedRanges.MinMax()
	blockSize := ns.Options().IndexOptions().BlockSize()
	blockStart := min.Truncate(blockSize)
	blockEnd := blockStart.Add(blockSize)
	expectedRangeStart, expectedRangeEnd := blockStart, blockEnd

	// Index blocks can be arbitrarily larger than data blocks, but the
	// retention of the namespace is based on the size of the data blocks,
	// not the index blocks. As a result, it's possible that the block start
	// for the earliest index block is before the earliest possible retention
	// time.
	// If that is the case, then we snap the expected range start to the
	// earliest retention block start because that is the point in time for
	// which we'll actually have data available to construct a segment from.
	//
	// Example:
	//  Index block size: 4 hours
	//  Data block size: 2 hours
	//  Retention: 6 hours
	//           [12PM->2PM][2PM->4PM][4PM->6PM] (Data Blocks)
	// [10AM     ->     2PM][2PM     ->     6PM] (Index Blocks)
	retentionOpts := ns.Options().RetentionOptions()
	nowFn := resultOpts.ClockOptions().NowFn()
	earliestRetentionTime := retention.FlushTimeStart(retentionOpts, nowFn())
	if blockStart.Before(earliestRetentionTime) {
		expectedRangeStart = earliestRetentionTime
	}

	expectedRanges := make(result.ShardTimeRanges, len(requestedRanges))
	for shard := range requestedRanges {
		expectedRanges[shard] = xtime.NewRanges(xtime.Range{
			Start: expectedRangeStart,
			End:   expectedRangeEnd,
		})
	}

	indexBlock, ok := indexResults[xtime.ToUnixNano(blockStart)]
	if !ok {
		// NB(bodu): We currently write empty data files to disk, which means that we can attempt to bootstrap
		// time ranges that have no data and no index block.
		// For example:
		// - peers data bootstrap from peer nodes receives peer blocks w/ no data (empty)
		// - peers data bootstrap writes empty ts data files to disk
		// - peers index bootstrap reads empty ts data files md from disk
		// - attempt to bootstrap time ranges that have no index results block
		return fmt.Errorf("could not find index block in results: time=%s, ts=%d",
			blockStart.String(), blockStart.UnixNano())
	}
	if len(builder.Docs()) == 0 {
		// No-op if there are no documents that ned to be written for this time block (nothing to persist).
		return nil
	}

	compactor.Lock()
	defer compactor.Unlock()
	seg, err := compactor.Compactor.CompactUsingBuilder(builder, nil, mmap.ReporterOptions{
		Context: mmap.Context{
			Name: mmapBootstrapIndexName,
		},
		Reporter: mmapReporter,
	})
	if err != nil {
		return err
	}

	segments := []segment.Segment{NewSegment(seg, false)}
	for _, seg := range indexBlock.Segments() {
		segments = append(segments, seg)
	}

	// Now replace the active segment with the built segment.
	newFulfilled := indexBlock.Fulfilled().Copy()
	newFulfilled.AddRanges(expectedRanges)
	replacedBlock := result.NewIndexBlock(blockStart, segments, newFulfilled)
	indexResults[xtime.ToUnixNano(blockStart)] = replacedBlock
	return nil
}
