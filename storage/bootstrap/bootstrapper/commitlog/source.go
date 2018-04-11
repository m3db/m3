// Copyright (c) 2016 Uber Technologies, Inc.
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

package commitlog

import (
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/persist/fs/msgpack"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

const encoderChanBufSize = 1000

type newIteratorFn func(opts commitlog.IteratorOpts) (commitlog.Iterator, error)

type commitLogSource struct {
	opts Options
	log  xlog.Logger

	// Mockable for testing
	newIteratorFn   newIteratorFn
	snapshotFilesFn snapshotFilesFn
	snapshotTimeFn  snapshotTimeFn
	newReaderFn     newReaderFn
}

type encoder struct {
	lastWriteAt time.Time
	enc         encoding.Encoder
}

type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.SnapshotFilesSlice, error)
type snapshotTimeFn func(filePathPrefix string, id fs.FilesetFileIdentifier, readerBufferSize int, decoder *msgpack.Decoder) (time.Time, error)
type newReaderFn func(bytesPool pool.CheckedBytesPool, opts fs.Options) (fs.FileSetReader, error)

func newCommitLogSource(opts Options) bootstrap.Source {
	return &commitLogSource{
		opts: opts,
		log:  opts.ResultOptions().InstrumentOptions().Logger(),

		newIteratorFn:   commitlog.NewIterator,
		snapshotFilesFn: fs.SnapshotFiles,
		snapshotTimeFn:  fs.SnapshotTime,
		newReaderFn:     fs.NewReader,
	}
}
func (s *commitLogSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *commitLogSource) Available(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Commit log bootstrapper is a last ditch effort, so fulfill all
	// time ranges requested even if not enough data, just to succeed
	// the bootstrap
	return shardsTimeRanges
}

type shardResultAndShard struct {
	shard  uint32
	result result.ShardResult
}

// Read will read a combination of the available snapshot files and commit log files to restore
// as much unflushed data from disk as possible. The logic for performing this correctly is as
// follows:
//
// 1. For every shard/blockStart combination, find the most recently written and complete (I.E
//    has a checkpoint file) snapshot. Bootstrap that file.
// 2. For every shard/blockStart combination, determine the SnapshotTime time for the snapshot file.
//    This value corresponds to the (local) moment in time right before the snapshotting process
// 	  began.
// 3. Find the minimum SnapshotTime time for all of the shards and block starts (call it t0), and replay all
//    commit log entries starting at t0.Add(-max(bufferPast, bufferFuture)). Note that commit log entries should be filtered
//    by the local system timestamp for when they were written, not for the timestamp of the data point
//    itself.
//
// The rationale for this is that for a given shard / block, if we have a snapshot file that was
// written at t0, then we can be guaranteed that the snapshot file contains every write for that
// shard/blockStart up until (t0 - max(bufferPast, bufferFuture)). Lets start by imagining a
// scenario where taking into account the bufferPast value is important:
//
// BlockSize: 2hr, bufferPast: 5m, bufferFuture: 20m
// Trying to bootstrap shard 0 for time period 12PM -> 2PM
// Snapshot file was written at 1:50PM then:
//
// Snapshot file contains all writes for (shard 0, blockStart 12PM) up until 1:45PM (1:50-5)
// because we started snapshotting at 1:50PM and a write at 1:50:01PM for a datapoint at 1:45PM would
// be rejected for trying to write too far into the past.
//
// As a result, we might conclude that reading the commit log from 1:45PM onwards would be sufficient,
// however, we also need to consider the value of bufferFuture. Reading the commit log starting at
// 1:45PM actually would not be sufficient because we could have received a write at 1:42 system-time
// (within the 20m bufferFuture range) for a datapoint at 2:02PM. This write would belong to the 2PM
// block, not the 12PM block, and as a result would not be captured in the snapshot file, because snapshot
// files are block-specific. As a result, we actually need to read everything in the commit log starting
// from 1:30PM (1:50-20).
// TODO: Diagram
func (s *commitLogSource) Read(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	var (
		snapshotFilesByShard = map[uint32]fs.SnapshotFilesSlice{}
		fsOpts               = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix       = fsOpts.FilePathPrefix()
	)
	for shard := range shardsTimeRanges {
		snapshotFiles, err := s.snapshotFilesFn(filePathPrefix, ns.ID(), shard)
		if err != nil {
			return nil, err
		}
		snapshotFilesByShard[shard] = snapshotFiles
	}

	// TODO: This is very parallelizable
	var (
		bopts                = s.opts.ResultOptions()
		bytesPool            = bopts.DatabaseBlockOptions().BytesPool()
		blocksPool           = bopts.DatabaseBlockOptions().DatabaseBlockPool()
		blockSize            = ns.Options().RetentionOptions().BlockSize()
		snapshotShardResults = make(map[uint32]result.ShardResult)
	)

	// For each block that we're bootstrapping, we need to figure out the most recent snapshot that
	// was taken for each shard. I.E we want to create a datastructure that looks like this:
	// map[blockStart]map[shard]mostRecentSnapshotTime
	// TODO: Rename variable to match method or vice versa
	// TODO: snapshots->snapshot
	mostRecentSnapshotsByBlockShard := s.mostRecentCompleteSnapshotPerShard(
		shardsTimeRanges, blockSize, snapshotFilesByShard, s.opts.CommitLogOptions().FilesystemOptions())

	// Once we have the desired datastructure, we next need to figure out the minimum most recent snapshot
	// for that block across all shards. This will help us determine how much of the commit log we need to
	// read. The new datastructure we're trying to generate looks like:
	// map[blockStart]minimumMostRecentSnapshot (accross all shards)
	// This structure is important because it tells us how much of the commit log we need to read for each
	// block that we're trying to bootstrap (because the commit log is shared across all shards).
	minimumMostRecentSnapshotByBlock := s.minimumMostRecentSnapshotByBlock(
		shardsTimeRanges, blockSize, mostRecentSnapshotsByBlockShard)

	// Now that we have the minimum most recent snapshot for each block, we can use that data to decide how
	// much of the commit log we need to read for each block that we're bootstrapping, but first we begin
	// by reading the snapshot files that we can.
	// TODO: Maybe do this before other calculations above?
	snapshotShardResults, err := s.bootstrapAvailableSnapshotFiles(
		ns.ID(), shardsTimeRanges, blockSize, snapshotFilesByShard, fsOpts, bytesPool, blocksPool)
	if err != nil {
		return nil, err
	}

	// At this point we've bootstrapped all the snapshot files that we can, and we need
	// to decide which commitlogs to read. We'll construct a new predicate based on the
	// data-structure we constructed earlier where the new predicate will check if there
	// is any overlap between a commit log file and a temporary range we construct that
	// begins with the minimum snapshot time and ends with the end of that block
	var (
		bufferPast   = ns.Options().RetentionOptions().BufferPast()
		bufferFuture = ns.Options().RetentionOptions().BufferFuture()
	)
	rangesToCheck := []xtime.Range{}
	for blockStart, minimumMostRecentSnapshotTime := range minimumMostRecentSnapshotByBlock {
		maxBufferPastAndFuture := math.Max(float64(int(bufferPast)), float64(int(bufferFuture)))

		rangesToCheck = append(rangesToCheck, xtime.Range{
			// We have to subtract Max(bufferPast, bufferFuture) for the reasons described
			// in the method documentation.
			Start: minimumMostRecentSnapshotTime.Add(-time.Duration(maxBufferPastAndFuture)),
			End:   blockStart.ToTime().Add(blockSize),
		})
	}

	// TODO: Test this logic (maybe in a helper FN?)
	readCommitlogPred := func(commitLogFileStart time.Time, commitLogFileBlockSize time.Duration) bool {
		// Note that the rangesToCheck that we generated earlier are *logical* ranges not
		// physical ones. I.E a range of 12:30PM to 2:00PM means that we need all data with
		// a timestamp between 12:30PM and 2:00PM which is strictly different than all datapoints
		// that *arrived* between 12:30PM and 2:00PM due to the bufferFuture and bufferPast
		// semantics.
		// Since the commitlog file ranges represent physical ranges, we will first convert them
		// to logical ranges, and *then* we will perform a range overlap comparison.
		for _, rangeToCheck := range rangesToCheck {
			commitLogEntryRange := xtime.Range{
				// Commit log filetime and duration represent system time, not the logical timestamps
				// of the values contained within.
				// Imagine the following scenario:
				// 		Namespace blockSize: 2 hours
				// 		Namespace bufferPast: 10 minutes
				// 		Namespace bufferFuture: 20 minutes
				// 		Commitlog file start: 12:30PM
				// 		Commitlog file blockSize: 15 minutes
				//
				// While the commitlog file only contains writes that were physically received
				// between 12:30PM and 12:45PM system-time, it *could* contain datapoints with
				// *logical* timestamps anywhere between 12:20PM and 1:05PM.
				//
				// I.E A write that arrives at exactly 12:30PM (system) with a timestamp of 12:20PM
				// (logical) would be within the 10 minute bufferPast period. Similarly, a write that
				// arrives at exactly 12:45PM (system) with a timestamp of 1:05PM (logical) would be
				// within the 20 minute bufferFuture period.
				Start: commitLogFileStart.Add(-bufferPast),
				End:   commitLogFileStart.Add(commitLogFileBlockSize).Add(bufferFuture),
			}

			if commitLogEntryRange.Overlaps(rangeToCheck) {
				return true
			}
		}

		return false
	}

	readSeriesPredicate := newReadSeriesPredicate(ns)
	iterOpts := commitlog.IteratorOpts{
		CommitLogOptions:      s.opts.CommitLogOptions(),
		FileFilterPredicate:   readCommitlogPred,
		SeriesFilterPredicate: readSeriesPredicate,
	}
	iter, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}

	defer iter.Close()

	var (
		// +1 so we can use the shard number as an index throughout without constantly
		// remembering to subtract 1 to convert to zero-based indexing
		numShards   = s.findHighestShard(shardsTimeRanges) + 1
		numConc     = s.opts.EncodingConcurrency()
		blopts      = bopts.DatabaseBlockOptions()
		encoderPool = bopts.DatabaseBlockOptions().EncoderPool()
		workerErrs  = make([]int, numConc)
	)

	unmerged := make([]encodersAndRanges, numShards)
	for shard := range shardsTimeRanges {
		unmerged[shard] = encodersAndRanges{
			encodersBySeries: make(map[uint64]encodersByTime),
			ranges:           shardsTimeRanges[shard],
		}
	}

	encoderChans := make([]chan encoderArg, numConc)
	for i := 0; i < numConc; i++ {
		encoderChans[i] = make(chan encoderArg, encoderChanBufSize)
	}

	// Spin up numConc background go-routines to handle M3TSZ encoding. This must
	// happen before we start reading to prevent infinitely blocking writes to
	// the encoderChans.
	wg := &sync.WaitGroup{}
	for workerNum, encoderChan := range encoderChans {
		wg.Add(1)
		go s.startM3TSZEncodingWorker(
			workerNum,
			encoderChan,
			unmerged,
			encoderPool,
			workerErrs,
			blopts,
			wg,
		)
	}

	for iter.Next() {
		series, dp, unit, annotation := iter.Current()
		// continue
		if !s.shouldEncodeSeries(unmerged, blockSize, series, dp) {
			continue
		}

		// Distribute work such that each encoder goroutine is responsible for
		// approximately numShards / numConc shards. This also means that all
		// datapoints for a given shard/series will be processed in a serialized
		// manner.
		// We choose to distribute work by shard instead of series.UniqueIndex
		// because it means that all accesses to the unmerged slice don't need
		// to be synchronized because each index belongs to a single shard so it
		// will only be accessed serially from a single worker routine.
		encoderChans[series.Shard%uint32(numConc)] <- encoderArg{
			series:     series,
			dp:         dp,
			unit:       unit,
			annotation: annotation,
			blockStart: dp.Timestamp.Truncate(blockSize),
		}
	}

	for _, encoderChan := range encoderChans {
		close(encoderChan)
	}

	// Block until all data has been read and encoded by the worker goroutines
	wg.Wait()
	s.logEncodingOutcome(workerErrs, iter)

	commitLogBootstrapResult := s.mergeShards(int(numShards), bopts, blopts, encoderPool, unmerged, snapshotShardResults)
	// TODO: Need to do a proper merge here
	commitLogShardResults := commitLogBootstrapResult.ShardResults()
	for shard, shardResult := range snapshotShardResults {
		existingShardResult, ok := commitLogShardResults[shard]
		if !ok {
			commitLogBootstrapResult.Add(shard, shardResult, xtime.Ranges{})
			continue
		}

		for _, series := range shardResult.AllSeries() {
			for blockStart, dbBlock := range series.Blocks.AllBlocks() {
				existingBlock, ok := existingShardResult.BlockAt(series.ID, blockStart.ToTime())
				if !ok {
					existingShardResult.AddBlock(series.ID, dbBlock)
					continue
				}

				existingBlock.Merge(dbBlock)
			}
		}
	}

	return commitLogBootstrapResult, nil
}

// mostRecentCompleteSnapshotPerShard returns the most recent (i.e latest) complete (i.e has a checkpoint file)
// snapshot for every blockStart/shard combination that we're trying to bootstrap. It returns a data structure
// that looks like map[blockStart]map[shard]mostRecentCompleteSnapshotTime
func (s *commitLogSource) mostRecentCompleteSnapshotPerShard(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	snapshotFilesByShard map[uint32]fs.SnapshotFilesSlice,
	fsOpts fs.Options,
) map[xtime.UnixNano]map[uint32]time.Time {
	minBlock, maxBlock := shardsTimeRanges.MinMax()
	decoder := msgpack.NewDecoder(nil)
	mostRecentSnapshotsByBlockShard := map[xtime.UnixNano]map[uint32]time.Time{}
	for currBlock := minBlock.Truncate(blockSize); currBlock.Before(maxBlock); currBlock = currBlock.Add(blockSize) {
		for shard := range shardsTimeRanges {
			func() {
				var (
					mostRecentSnapshotTime time.Time
					mostRecentSnapshot     fs.SnapshotFile
					err                    error
				)

				defer func() {
					existing := mostRecentSnapshotsByBlockShard[xtime.ToUnixNano(currBlock)]
					if existing == nil {
						existing = map[uint32]time.Time{}
					}
					existing[shard] = mostRecentSnapshotTime
					mostRecentSnapshotsByBlockShard[xtime.ToUnixNano(currBlock)] = existing
				}()

				snapshotFiles, ok := snapshotFilesByShard[shard]
				if !ok {
					// If there are no snapshot files for this shard, then for this block we will
					// need to read the entire commit log for that period so we just set the most
					// recent snapshot to the beginning of the block.
					mostRecentSnapshotTime = currBlock
					return
				}

				mostRecentSnapshot, ok = snapshotFiles.LatestValidForBlock(currBlock)
				if !ok {
					// If there are no snapshot files for this block, then for this block we will
					// need to read the entire commit log for that period so we just set the most
					// recent snapshot to the beginning of the block.
					mostRecentSnapshotTime = currBlock
					return
				}

				var (
					filePathPrefix       = s.opts.CommitLogOptions().FilesystemOptions().FilePathPrefix()
					infoReaderBufferSize = s.opts.CommitLogOptions().FilesystemOptions().InfoReaderBufferSize()
				)
				// Performs I/O
				mostRecentSnapshotTime, err = s.snapshotTimeFn(
					filePathPrefix, mostRecentSnapshot.ID, infoReaderBufferSize, decoder)
				if err != nil {
					// TODO: Emit error log
					// Can't read the snapshot file for this block, then we will need to read
					// the entire commit log for that period so we just set the most recent snapshot
					// to the beginning of the block
					mostRecentSnapshotTime = currBlock
					return
				}
			}()
		}
	}

	return mostRecentSnapshotsByBlockShard
}

func (s *commitLogSource) minimumMostRecentSnapshotByBlock(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	mostRecentSnapshotByBlockShard map[xtime.UnixNano]map[uint32]time.Time,
) map[xtime.UnixNano]time.Time {
	minimumMostRecentSnapshotByBlock := map[xtime.UnixNano]time.Time{}
	for blockStart, mostRecentSnapshotsByShard := range mostRecentSnapshotByBlockShard {
		minMostRecentSnapshot := blockStart.ToTime().Add(blockSize) // TODO: This is confusing, figure out how to explain it
		for shard, mostRecentSnapshotForShard := range mostRecentSnapshotsByShard {
			blockRange := xtime.Range{Start: blockStart.ToTime(), End: blockStart.ToTime().Add(blockSize)}
			if !shardsTimeRanges[shard].Overlaps(blockRange) {
				// In order for a minimum most recent snapshot to be valid, it needs to be for
				// a block that we need to actually bootstrap for that shard. This check may
				// seem unnecessary, but it ensures that our algorithm is correct even if we're
				// bootstrapping different blocks for various shards.
				continue
			}
			if mostRecentSnapshotForShard.Before(minMostRecentSnapshot) {
				minMostRecentSnapshot = mostRecentSnapshotForShard
			}
		}
		minimumMostRecentSnapshotByBlock[blockStart] = minMostRecentSnapshot
	}

	return minimumMostRecentSnapshotByBlock
}

func (s *commitLogSource) bootstrapAvailableSnapshotFiles(
	nsID ident.ID,
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	snapshotFilesByShard map[uint32]fs.SnapshotFilesSlice,
	fsOpts fs.Options,
	bytesPool pool.CheckedBytesPool,
	blocksPool block.DatabaseBlockPool,
) (map[uint32]result.ShardResult, error) {
	snapshotShardResults := make(map[uint32]result.ShardResult)

	for shard, tr := range shardsTimeRanges {
		rangeIter := tr.Iter()
		for hasMore := rangeIter.Next(); hasMore; hasMore = rangeIter.Next() {
			currRange := rangeIter.Value()

			// TODO: Clean this up
			if (currRange.End.Unix()-currRange.Start.Unix())/int64(blockSize) != 0 {
				return nil, fmt.Errorf(
					"received bootstrap range that is not multiple of blocksize, blockSize: %d, start: %d, end: %d",
					blockSize, currRange.End.Unix(), currRange.Start.Unix())
			}

			// TODO: Add function for this iteration
			// TODO: Estimate capacity better
			shardResult := result.NewShardResult(0, s.opts.ResultOptions())
			for blockStart := currRange.Start.Truncate(blockSize); blockStart.Before(currRange.End); blockStart = blockStart.Add(blockSize) {
				snapshotFiles := snapshotFilesByShard[shard]

				// TODO: Already called this FN, maybe should just re-use results somehow?
				latestSnapshot, ok := snapshotFiles.LatestValidForBlock(blockStart)
				if !ok {
					// TODO: Ensure that the minimum is properly set for this shard/block
					// There is no snapshot file for this shard / block combination
					continue
				}

				// Bootstrap the snapshot file
				reader, err := s.newReaderFn(bytesPool, fsOpts)
				if err != nil {
					// TODO: Probably don't want to return an err here, just emit a log and then
					// adjust the minimum for the shard/block to read from commit log
					// Actually maybe thats not true cause we may have deleted the commit log...
					// Return unfulfilled?
					return nil, err
				}
				err = reader.Open(fs.ReaderOpenOptions{
					Identifier: fs.FilesetFileIdentifier{
						Namespace:  nsID,
						BlockStart: blockStart,
						Shard:      shard,
						Index:      latestSnapshot.ID.Index,
					},
					FilesetType: persist.FilesetSnapshotType,
				})
				if err != nil {
					// TODO: Probably don't want to return an err here, just emit a log and then
					// adjust the minimum for the shard/block to read from commit log
					// Actually maybe thats not true cause we may have deleted the commit log...
					// Return unfulfilled?
					return nil, err
				}

				for {
					// TODO: Verify checksum?
					id, data, _, err := reader.Read()
					if err != nil && err != io.EOF {
						return nil, err
					}

					if err == io.EOF {
						break
					}

					dbBlock := blocksPool.Get()
					dbBlock.Reset(blockStart, ts.NewSegment(data, nil, ts.FinalizeHead))
					shardResult.AddBlock(id, dbBlock)
				}
			}
			snapshotShardResults[shard] = shardResult
		}
	}

	return snapshotShardResults, nil
}

func (s *commitLogSource) startM3TSZEncodingWorker(
	workerNum int,
	ec <-chan encoderArg,
	unmerged []encodersAndRanges,
	encoderPool encoding.EncoderPool,
	workerErrs []int,
	blopts block.Options,
	wg *sync.WaitGroup,
) {
	for arg := range ec {
		var (
			series     = arg.series
			dp         = arg.dp
			unit       = arg.unit
			annotation = arg.annotation
			blockStart = arg.blockStart
		)

		unmergedShard := unmerged[series.Shard].encodersBySeries
		unmergedSeries, ok := unmergedShard[series.UniqueIndex]
		if !ok {
			unmergedSeries = encodersByTime{
				id:       series.ID,
				encoders: make(map[xtime.UnixNano]encoders)}
			unmergedShard[series.UniqueIndex] = unmergedSeries
		}

		var (
			err            error
			blockStartNano = xtime.ToUnixNano(blockStart)
			unmergedBlock  = unmergedSeries.encoders[blockStartNano]
			wroteExisting  = false
		)
		for i := range unmergedBlock {
			if unmergedBlock[i].lastWriteAt.Before(dp.Timestamp) {
				unmergedBlock[i].lastWriteAt = dp.Timestamp
				err = unmergedBlock[i].enc.Encode(dp, unit, annotation)
				wroteExisting = true
				break
			}
		}
		if !wroteExisting {
			enc := encoderPool.Get()
			enc.Reset(blockStart, blopts.DatabaseBlockAllocSize())

			err = enc.Encode(dp, unit, annotation)
			if err == nil {
				unmergedBlock = append(unmergedBlock, encoder{
					lastWriteAt: dp.Timestamp,
					enc:         enc,
				})
				unmergedSeries.encoders[blockStartNano] = unmergedBlock
			}
		}
		if err != nil {
			workerErrs[workerNum]++
		}
	}
	wg.Done()
}

func (s *commitLogSource) shouldEncodeSeries(
	unmerged []encodersAndRanges,
	blockSize time.Duration,
	series commitlog.Series,
	dp ts.Datapoint,
) bool {
	// Check if the shard number is higher the amount of space we pre-allocated.
	// If it is, then it's not one of the shards we're trying to bootstrap
	if series.Shard > uint32(len(unmerged)-1) {
		return false
	}

	// Check if the shard is one of the shards we're trying to bootstrap
	ranges := unmerged[series.Shard].ranges
	if ranges.IsEmpty() {
		// Did not allocate map for this shard so not expecting data for it
		return false
	}

	// Check if the block corresponds to the time-range that we're trying to bootstrap
	blockStart := dp.Timestamp.Truncate(blockSize)
	blockEnd := blockStart.Add(blockSize)
	blockRange := xtime.Range{
		Start: blockStart,
		End:   blockEnd,
	}

	return ranges.Overlaps(blockRange)
}

func (s *commitLogSource) mergeShards(
	numShards int,
	bopts result.Options,
	blopts block.Options,
	encoderPool encoding.EncoderPool,
	unmerged []encodersAndRanges,
	snapshotShardResults map[uint32]result.ShardResult,
) result.BootstrapResult {
	var (
		shardErrs               = make([]int, numShards)
		shardEmptyErrs          = make([]int, numShards)
		bootstrapResult         = result.NewBootstrapResult()
		blocksPool              = bopts.DatabaseBlockOptions().DatabaseBlockPool()
		multiReaderIteratorPool = blopts.MultiReaderIteratorPool()
		// Controls how many shards can be merged in parallel
		workerPool          = xsync.NewWorkerPool(s.opts.MergeShardsConcurrency())
		bootstrapResultLock sync.Mutex
		wg                  sync.WaitGroup
	)

	workerPool.Init()

	for shard, unmergedShard := range unmerged {
		if unmergedShard.encodersBySeries == nil {
			continue
		}
		wg.Add(1)
		shard, unmergedShard := shard, unmergedShard
		mergeShardFunc := func() {
			var shardResult result.ShardResult
			// TODO: Fix this possibly nil map
			shardResult, shardEmptyErrs[shard], shardErrs[shard] = s.mergeShard(
				unmergedShard, snapshotShardResults[uint32(shard)], blocksPool, multiReaderIteratorPool, encoderPool, blopts)
			if shardResult != nil && len(shardResult.AllSeries()) > 0 {
				// Prevent race conditions while updating bootstrapResult from multiple go-routines
				bootstrapResultLock.Lock()
				// Shard is a slice index so conversion to uint32 is safe
				bootstrapResult.Add(uint32(shard), shardResult, xtime.Ranges{})
				bootstrapResultLock.Unlock()
			}
			wg.Done()
		}
		workerPool.Go(mergeShardFunc)
	}

	// Wait for all merge goroutines to complete
	wg.Wait()
	s.logMergeShardsOutcome(shardErrs, shardEmptyErrs)
	return bootstrapResult
}

func (s *commitLogSource) mergeShard(
	unmergedShard encodersAndRanges,
	snapshotShardResult result.ShardResult,
	blocksPool block.DatabaseBlockPool,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
	encoderPool encoding.EncoderPool,
	blopts block.Options,
) (result.ShardResult, int, int) {
	var shardResult result.ShardResult
	var numShardEmptyErrs int
	var numErrs int

	for _, unmergedBlocks := range unmergedShard.encodersBySeries {
		seriesBlocks, numSeriesEmptyErrs, numSeriesErrs := s.mergeSeries(
			unmergedBlocks,
			blocksPool,
			multiReaderIteratorPool,
			encoderPool,
			blopts,
		)

		// for blockStart, block := range seriesBlocks.AllBlocks() {
		// 	snapshotBlock, ok := snapshotShardResult.BlockAt(unmergedBlocks.id, blockStart.ToTime())
		// 	if !ok {
		// 		continue
		// 	}

		// 	block.Merge(snapshotBlock)
		// }

		if seriesBlocks != nil && seriesBlocks.Len() > 0 {
			if shardResult == nil {
				shardResult = result.NewShardResult(len(unmergedShard.encodersBySeries), s.opts.ResultOptions())
			}
			shardResult.AddSeries(unmergedBlocks.id, seriesBlocks)
		}

		numShardEmptyErrs += numSeriesEmptyErrs
		numErrs += numSeriesErrs
	}
	return shardResult, numShardEmptyErrs, numErrs
}

func (s *commitLogSource) mergeSeries(
	unmergedBlocks encodersByTime,
	blocksPool block.DatabaseBlockPool,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
	encoderPool encoding.EncoderPool,
	blopts block.Options,
) (block.DatabaseSeriesBlocks, int, int) {
	var seriesBlocks block.DatabaseSeriesBlocks
	var numEmptyErrs int
	var numErrs int

	for startNano, encoders := range unmergedBlocks.encoders {
		start := startNano.ToTime()

		if len(encoders) == 0 {
			numEmptyErrs++
			continue
		}

		if len(encoders) == 1 {
			pooledBlock := blocksPool.Get()
			pooledBlock.Reset(start, encoders[0].enc.Discard())
			if seriesBlocks == nil {
				seriesBlocks = block.NewDatabaseSeriesBlocks(len(unmergedBlocks.encoders))
			}
			seriesBlocks.AddBlock(pooledBlock)
			continue
		}

		// Convert encoders to readers so we can use iteration helpers
		readers := encoders.newReaders()
		iter := multiReaderIteratorPool.Get()
		iter.Reset(readers)

		var err error
		enc := encoderPool.Get()
		enc.Reset(start, blopts.DatabaseBlockAllocSize())
		for iter.Next() {
			dp, unit, annotation := iter.Current()
			encodeErr := enc.Encode(dp, unit, annotation)
			if encodeErr != nil {
				err = encodeErr
				numErrs++
				break
			}
		}

		if iterErr := iter.Err(); iterErr != nil {
			if err == nil {
				err = iter.Err()
			}
			numErrs++
		}

		// Automatically returns iter to the pool
		iter.Close()
		encoders.close()
		readers.close()

		if err != nil {
			continue
		}

		pooledBlock := blocksPool.Get()
		pooledBlock.Reset(start, enc.Discard())
		if seriesBlocks == nil {
			seriesBlocks = block.NewDatabaseSeriesBlocks(len(unmergedBlocks.encoders))
		}
		seriesBlocks.AddBlock(pooledBlock)
	}
	return seriesBlocks, numEmptyErrs, numErrs
}

func (s *commitLogSource) findHighestShard(shardsTimeRanges result.ShardTimeRanges) uint32 {
	var max uint32
	for shard := range shardsTimeRanges {
		if shard > max {
			max = shard
		}
	}
	return max
}

func (s *commitLogSource) logEncodingOutcome(workerErrs []int, iter commitlog.Iterator) {
	errSum := 0
	for _, numErrs := range workerErrs {
		errSum += numErrs
	}
	if errSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d block encode errors", errSum)
	}
	if err := iter.Err(); err != nil {
		s.log.Errorf("error reading commit log: %v", err)
	}
}

func (s *commitLogSource) logMergeShardsOutcome(shardErrs []int, shardEmptyErrs []int) {
	errSum := 0
	for _, numErrs := range shardErrs {
		errSum += numErrs
	}
	if errSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d merge out of order errors", errSum)
	}

	emptyErrSum := 0
	for _, numEmptyErr := range shardEmptyErrs {
		emptyErrSum += numEmptyErr
	}
	if emptyErrSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d empty unmerged blocks errors", emptyErrSum)
	}
}

func newReadCommitLogPredicate(
	ns namespace.Metadata,
	shardRange xtime.Range,
	opts Options,
) commitlog.FileFilterPredicate {
	// How far into the past or future a commitlog might contain a write for a
	// previous or future block
	bufferPast := ns.Options().RetentionOptions().BufferPast()
	bufferFuture := ns.Options().RetentionOptions().BufferFuture()

	return func(entryTime time.Time, entryDuration time.Duration) bool {
		// If there is any amount of overlap between the commitlog range and the
		// shardRange then we need to read the commitlog file
		return xtime.Range{
			Start: entryTime.Add(-bufferPast),
			End:   entryTime.Add(entryDuration).Add(bufferFuture),
		}.Overlaps(shardRange)
	}
}

func newReadSeriesPredicate(ns namespace.Metadata) commitlog.SeriesFilterPredicate {
	nsID := ns.ID()
	return func(id ident.ID, namespace ident.ID) bool {
		return nsID.Equal(namespace)
	}
}

type encodersAndRanges struct {
	encodersBySeries map[uint64]encodersByTime
	ranges           xtime.Ranges
}

type encodersByTime struct {
	id ident.ID
	// int64 instead of time.Time because there is an optimized map access pattern
	// for i64's
	encoders map[xtime.UnixNano]encoders
}

// encoderArg contains all the information a worker go-routine needs to encode
// a data point as M3TSZ
type encoderArg struct {
	series     commitlog.Series
	dp         ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
	blockStart time.Time
}

type encoders []encoder

type ioReaders []io.Reader

func (e encoders) newReaders() ioReaders {
	readers := make(ioReaders, len(e))
	for i := range e {
		readers[i] = e[i].enc.Stream()
	}
	return readers
}

func (e encoders) close() {
	for i := range e {
		e[i].enc.Close()
	}
}

func (ir ioReaders) close() {
	for _, r := range ir {
		r.(xio.SegmentReader).Finalize()
	}
}
