package bootstrapper

import (
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	xtime "github.com/m3db/m3/src/x/time"
)

// SharedPersistManager is a lockable persist manager that's safe to be shared across threads.
type SharedPersistManager struct {
	sync.Mutex
	Mgr persist.Manager
}

// PersistBootstrapIndexSegment is a helper function that persists bootstrapped index segments for a ns -> block of time.
func PersistBootstrapIndexSegment(
	ns namespace.Metadata,
	requestedRanges result.ShardTimeRanges,
	indexResults result.IndexResults,
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
		expectedRanges[shard] = xtime.Ranges{}.AddRange(xtime.Range{
			Start: expectedRangeStart,
			End:   expectedRangeEnd,
		})
	}

	indexBlock, ok := indexResults[xtime.ToUnixNano(blockStart)]
	if !ok {
		return fmt.Errorf("did not find index block for blocksStart: %d", blockStart.Unix())
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
		segments := persistedSegments
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

	if err := preparedPersist.Persist(indexBlock.Builder()); err != nil {
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

type writeLock interface {
	Lock()
	Unlock()
}

// CreateFlushBatchFn creates a batch flushing fn for code reuse.
func CreateFlushBatchFn(
	mu writeLock,
	batch []doc.Document,
	builder segment.DocumentsBuilder,
) func() error {
	return func() error {
		if len(batch) == 0 {
			// Last flush might not have any docs enqueued
			return nil
		}

		// NB(bodu): Prevent concurrent writes.
		// Although it seems like there's no need to lock on writes since
		// each block should ONLY be getting built in a single thread.
		mu.Lock()
		err := builder.InsertBatch(index.Batch{
			Docs:                batch,
			AllowPartialUpdates: true,
		})
		mu.Unlock()
		if err != nil && index.IsBatchPartialError(err) {
			// If after filtering out duplicate ID errors
			// there are no errors, then this was a successful
			// insertion.
			batchErr := err.(*index.BatchPartialError)
			// NB(r): FilterDuplicateIDErrors returns nil
			// if no errors remain after filtering duplicate ID
			// errors, this case is covered in unit tests.
			err = batchErr.FilterDuplicateIDErrors()
		}
		if err != nil {
			return err
		}

		// Reset docs batch for reuse
		var empty doc.Document
		for i := range batch {
			batch[i] = empty
		}
		batch = batch[:0]
		return nil
	}
}
