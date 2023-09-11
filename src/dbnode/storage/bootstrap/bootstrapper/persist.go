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
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
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
	builder segment.DocumentsBuilder,
	persistManager *SharedPersistManager,
	indexClaimsManager fs.IndexClaimsManager,
	resultOpts result.Options,
	fulfilled result.ShardTimeRanges,
	blockStart xtime.UnixNano,
	blockEnd xtime.UnixNano,
) (result.IndexBlock, error) {
	// No-op if there are no documents that need to be written for this time block (nothing to persist).
	if len(builder.Docs()) == 0 {
		return result.IndexBlock{}, nil
	}

	// If we're performing an index run with persistence enabled
	// determine if we covered a full block exactly (which should
	// occur since we always group readers by block size).
	_, max := requestedRanges.MinMax()
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
	now := xtime.ToUnixNano(nowFn())
	earliestRetentionTime := retention.FlushTimeStart(retentionOpts, now)

	// If bootstrapping is taking more time than our retention period, we might end up in a situation
	// when earliestRetentionTime is larger than out block end time. This means that the blocks
	// got outdated during bootstrap so we just skip building index segments for them.
	if !blockEnd.After(earliestRetentionTime) {
		return result.IndexBlock{}, fs.ErrIndexOutOfRetention
	}

	if blockStart.Before(earliestRetentionTime) {
		expectedRangeStart = earliestRetentionTime
	}

	shards := make(map[uint32]struct{})
	expectedRanges := result.NewShardTimeRangesFromSize(requestedRanges.Len())
	for shard := range requestedRanges.Iter() {
		shards[shard] = struct{}{}
		expectedRanges.Set(shard, xtime.NewRanges(xtime.Range{
			Start: expectedRangeStart,
			End:   expectedRangeEnd,
		}))
	}

	return persistBootstrapIndexSegment(
		ns,
		shards,
		builder,
		persistManager,
		indexClaimsManager,
		requestedRanges,
		expectedRanges,
		fulfilled,
		blockStart,
		max,
	)
}

func persistBootstrapIndexSegment(
	ns namespace.Metadata,
	shards map[uint32]struct{},
	builder segment.DocumentsBuilder,
	persistManager *SharedPersistManager,
	indexClaimsManager fs.IndexClaimsManager,
	requestedRanges result.ShardTimeRanges,
	expectedRanges result.ShardTimeRanges,
	fulfilled result.ShardTimeRanges,
	blockStart xtime.UnixNano,
	max xtime.UnixNano,
) (result.IndexBlock, error) {
	// Check that we completely fulfilled all shards for the block
	// and we didn't bootstrap any more/less than expected.
	requireFulfilled := expectedRanges.Copy()
	requireFulfilled.Subtract(fulfilled)
	exactStartEnd := max.Equal(blockStart.Add(ns.Options().IndexOptions().BlockSize()))
	if !exactStartEnd || !requireFulfilled.IsEmpty() {
		return result.IndexBlock{}, fmt.Errorf("persistent fs index bootstrap invalid ranges to persist: "+
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
		return result.IndexBlock{}, err
	}

	var calledDone bool
	defer func() {
		if !calledDone {
			flush.DoneIndex()
		}
	}()

	volumeIndex, err := indexClaimsManager.ClaimNextIndexFileSetVolumeIndex(
		ns,
		blockStart,
	)
	if err != nil {
		return result.IndexBlock{}, fmt.Errorf("failed to claim next index volume index: %w", err)
	}

	preparedPersist, err := flush.PrepareIndex(persist.IndexPrepareOptions{
		NamespaceMetadata: ns,
		BlockStart:        blockStart,
		FileSetType:       persist.FileSetFlushType,
		Shards:            shards,
		// NB(bodu): Assume default volume type when persisted bootstrapped index data.
		IndexVolumeType: idxpersist.DefaultIndexVolumeType,
		VolumeIndex:     volumeIndex,
	})
	if err != nil {
		return result.IndexBlock{}, err
	}

	var calledClose bool
	defer func() {
		if !calledClose {
			preparedPersist.Close()
		}
	}()

	if err := preparedPersist.Persist(builder); err != nil {
		return result.IndexBlock{}, err
	}

	calledClose = true
	persistedSegments, err := preparedPersist.Close()
	if err != nil {
		return result.IndexBlock{}, err
	}

	calledDone = true
	if err := flush.DoneIndex(); err != nil {
		return result.IndexBlock{}, err
	}
	segments := make([]result.Segment, 0, len(persistedSegments))
	for _, pSeg := range persistedSegments {
		segments = append(segments, result.NewSegment(pSeg, true))
	}

	return result.NewIndexBlock(segments, expectedRanges), nil
}

// BuildBootstrapIndexSegment is a helper function that builds (in memory) bootstrapped index segments for a ns -> block of time.
func BuildBootstrapIndexSegment(
	ns namespace.Metadata,
	requestedRanges result.ShardTimeRanges,
	builder segment.DocumentsBuilder,
	compactor *SharedCompactor,
	resultOpts result.Options,
	mmapReporter mmap.Reporter,
	blockStart xtime.UnixNano,
	blockEnd xtime.UnixNano,
) (result.IndexBlock, error) {
	// No-op if there are no documents that need to be written for this time block (nothing to persist).
	if len(builder.Docs()) == 0 {
		return result.IndexBlock{}, nil
	}

	// If we're performing an index run with persistence enabled
	// determine if we covered a full block exactly (which should
	// occur since we always group readers by block size).
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
	//           [12PM->2PM)[2PM->4PM)[4PM->6PM) (Data Blocks)
	// [10AM     ->     2PM)[2PM     ->     6PM) (Index Blocks)
	retentionOpts := ns.Options().RetentionOptions()
	nowFn := resultOpts.ClockOptions().NowFn()
	now := xtime.ToUnixNano(nowFn())
	earliestRetentionTime := retention.FlushTimeStart(retentionOpts, now)

	// If bootstrapping is taking more time than our retention period, we might end up in a situation
	// when earliestRetentionTime is larger than out block end time. This means that the blocks
	// got outdated during bootstrap so we just skip building index segments for them.
	if !blockEnd.After(earliestRetentionTime) {
		return result.IndexBlock{}, fs.ErrIndexOutOfRetention
	}

	if blockStart.Before(earliestRetentionTime) {
		expectedRangeStart = earliestRetentionTime
	}

	expectedRanges := result.NewShardTimeRangesFromSize(requestedRanges.Len())
	for shard := range requestedRanges.Iter() {
		expectedRanges.Set(shard, xtime.NewRanges(xtime.Range{
			Start: expectedRangeStart,
			End:   expectedRangeEnd,
		}))
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
		return result.IndexBlock{}, err
	}

	segs := []result.Segment{result.NewSegment(seg, false)}
	indexResult := result.NewIndexBlock(segs, expectedRanges)
	return indexResult, nil
}

// GetDefaultIndexBlockForBlockStart gets the index block for the default volume type from the index results.
func GetDefaultIndexBlockForBlockStart(
	results result.IndexResults,
	blockStart xtime.UnixNano,
) (result.IndexBlock, bool) {
	indexBlockByVolumeType, ok := results[blockStart]
	if !ok {
		// NB(bodu): We currently write empty data files to disk, which means that we can attempt to bootstrap
		// time ranges that have no data and no index block.
		// For example:
		// - peers data bootstrap from peer nodes receives peer blocks w/ no data (empty)
		// - peers data bootstrap writes empty ts data files to disk
		// - peers index bootstrap reads empty ts data files md from disk
		// - attempt to bootstrap time ranges that have no index results block
		return result.IndexBlock{}, false
	}
	indexBlock, ok := indexBlockByVolumeType.GetBlock(idxpersist.DefaultIndexVolumeType)
	if !ok {
		return result.IndexBlock{}, false
	}
	return indexBlock, true
}
