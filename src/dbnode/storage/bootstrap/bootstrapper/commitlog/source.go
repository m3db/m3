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
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/persist"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3db/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/index/convert"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"
)

var (
	errIndexingNotEnableForNamespace = errors.New("indexing not enabled for namespace")
)

const encoderChanBufSize = 1000

type newIteratorFn func(opts commitlog.IteratorOpts) (commitlog.Iterator, error)
type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error)
type snapshotTimeFn func(filePathPrefix string, id fs.FileSetFileIdentifier, readerBufferSize int, decoder *msgpack.Decoder) (time.Time, error)
type newReaderFn func(bytesPool pool.CheckedBytesPool, opts fs.Options) (fs.DataFileSetReader, error)

type commitLogSource struct {
	opts Options
	log  xlog.Logger

	// Filesystem inspection capture before node was started.
	inspection fs.Inspection

	newIteratorFn   newIteratorFn
	snapshotFilesFn snapshotFilesFn
	snapshotTimeFn  snapshotTimeFn
	newReaderFn     newReaderFn

	cachedShardDataByNS map[string]*cachedShardData
}

type encoder struct {
	lastWriteAt time.Time
	enc         encoding.Encoder
}

func newCommitLogSource(opts Options, inspection fs.Inspection) bootstrap.Source {
	return &commitLogSource{
		opts: opts,
		log:  opts.ResultOptions().InstrumentOptions().Logger(),

		inspection: inspection,

		newIteratorFn:   commitlog.NewIterator,
		snapshotFilesFn: fs.SnapshotFiles,
		// snapshotTimeFn:
		newReaderFn: fs.NewReader,

		cachedShardDataByNS: map[string]*cachedShardData{},
	}
}

func (s *commitLogSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *commitLogSource) AvailableData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Commit log bootstrapper is a last ditch effort, so fulfill all
	// time ranges requested even if not enough data, just to succeed
	// the bootstrap
	return shardsTimeRanges
}

// ReadData will read a combination of the available snapshot filesand commit log files to
// restore as much unflushed data from disk as possible. The logic for performing this
// correct is as follows:
//
// 		1. For every shard/blockStart combination, find the most recently written and complete
// 		   (I.E has a checkpoint file) snapshot. Bootstrap that file.
// 		2. For every shard/blockStart combination, determine the SnapshotTime for the snapshot file.
// 		   This value corresponds to the (local) moment in time right before the snapshotting process
// 		   began.
// 		3. Find the minimum SnapshotTime for all of the shards and block starts (call it t0), and
// 		   replay all commit log entries starting at t0.Add(-max(bufferPast, bufferFuture)). Note that
//         commit log entries should be filtered by the local system timestamp for when they were written,
//         not for the timestamp of the data point itself.
//
// The rationale for this is that for a given shard / blockStart, if we have a snapshot file that was written
// at t0, then its guaranteed that the snapshot file contains every write for that shard/blockStart up until
// (t0 - max(bufferPast, bufferFuture)). Lets start by imagining a scenario where taking into account the bufferPast
// value is important:
//
// blockSize: 2hr, BufferPast: 5m, bufferFuture: 20m
// Trying to bootstrap shard 0 for time period 12PM -> 2PM
// Snapshot file was written at 1:50PM then:
//
// Snapshot file contains all writes for (shard 0, blockStart 12PM) up until 1:45PM (1:50-5) because we started
// snapshotting at 1:50PM and a write at 1:50:01PM for a datapoint at 1:45PM would be rejected for trying to write
// too far into the past.
//
// As a result, we might conclude that reading the commit log from 1:45PM onwards would be sufficient, however, we
// also need to consider the value of bufferFuture. Reading the commit log starting at 1:45PM would actually not be
// sufficient because we could have received a write at 1:42 system-time (within the 20m bufferFuture range) for a
// datapoint at 2:02PM. This write would belong to the 2PM block, not the 12PM block, and as a result would not be
// captured in the snapshot file, because snapshot files are block-specific. As a result, we actually need to read
// everything in the commit log starting from 1:30PM (1:50-20). <--- I'm not actually sure this is true, it seems
// like actually we just need to read everything in the commit log starting from 1:40PM (2:00-20)
//
//
// Also, why cant this all be simplified by just assuming that for a given block we need to read all things from the
// commitlog not covered by the snapshot files physical snapshot time?
// TODO: Diagram
func (s *commitLogSource) ReadData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	var (
		snapshotFilesByShard = map[uint32]fs.FileSetFilesSlice{}
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

	var (
		bOpts  = s.opts.ResultOptions()
		blOpts = bOpts.DatabaseBlockOptions()
		// bytesPool  = blOpts.BytesPool()
		// blocksPool = blOpts.DatabaseBlockPool()
		blockSize = ns.Options().RetentionOptions().BlockSize()
		// snapshotShardResults = make(map[uint32]result.ShardResult)
	)

	// Start off by bootstrapping the most recent and complete snapshot file for each
	// shard/blockStart combination.
	// snapshotShardResults, err := s.bootstrapAvailableSnapshotFiles(

	// )

	readCommitLogPredicate := newReadCommitLogPredicate(
		ns, shardsTimeRanges, s.opts, s.inspection)

	// TODO(rartoul): When we implement caching data across namespaces, this will need
	// to be commitlog.ReadAllSeriesPredicate() if CacheSeriesMetadata() is enabled
	// because we'll need to read data for all namespaces, not just the one we're currently
	// bootstrapping.
	readSeriesPredicate := newReadSeriesPredicate(ns)
	iterOpts := commitlog.IteratorOpts{
		CommitLogOptions:      s.opts.CommitLogOptions(),
		FileFilterPredicate:   readCommitLogPredicate,
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
		encoderPool = blOpts.EncoderPool()
		workerErrs  = make([]int, numConc)
	)

	shardDataByShard := make([]shardData, numShards)
	for shard := range shardsTimeRanges {
		shardDataByShard[shard] = shardData{
			series: make(map[uint64]metadataAndEncodersByTime),
			ranges: shardsTimeRanges[shard],
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
			ns, runOpts, workerNum, encoderChan, shardDataByShard, encoderPool, workerErrs, blOpts, wg)
	}

	for iter.Next() {
		series, dp, unit, annotation := iter.Current()
		if !s.shouldEncodeForData(shardDataByShard, blockSize, series, dp.Timestamp) {
			continue
		}

		// Distribute work such that each encoder goroutine is responsible for
		// approximately numShards / numConc shards. This also means that all
		// datapoints for a given shard/series will be processed in a serialized
		// manner.
		// We choose to distribute work by shard instead of series.UniqueIndex
		// because it means that all accesses to the shardDataByShard slice don't need
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

	result := s.mergeShards(int(numShards), bOpts, blockSize, blOpts, encoderPool, shardDataByShard)
	// After merging shards, its safe to cache the shardData (which involves some mutation).
	if s.shouldCacheSeriesMetadata(runOpts, ns) {
		s.cacheShardData(ns, shardDataByShard)
	}
	return result, nil
}

func (s *commitLogSource) mostRecentCompleteSnapshotTimeByBlockShard(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
	fsOpts fs.Options,
) map[xtime.UnixNano]map[uint32]time.Time {
	var (
		// TODO: Maybe add a IterateOverBlocks method to this data structure?
		minBlock, maxBlock              = shardsTimeRanges.MinMax()
		decoder                         = msgpack.NewDecoder(nil)
		mostRecentSnapshotsByBlockShard = map[xtime.UnixNano]map[uint32]time.Time{}
	)

	for currBlockStart := minBlock.Truncate(blockSize); currBlockStart.Before(maxBlock); currBlockStart = currBlockStart.Add(blockSize) {
		for shard := range shardsTimeRanges {
			func() {
				var (
					currBlockUnixNanos     = xtime.ToUnixNano(currBlockStart)
					mostRecentSnapshotTime time.Time
					mostRecentSnapshot     fs.FileSetFile
					err                    error
				)

				defer func() {
					existing := mostRecentSnapshotsByBlockShard[currBlockUnixNanos]
					if existing == nil {
						existing = map[uint32]time.Time{}
					}

					if mostRecentSnapshotTime.IsZero() {
						// If we were unable to determine the most recent snapshot time for a given
						// shard/blockStart combination, then just fall back to using the blockStart
						// time as that will force us to read the entire commit log for that duration.
						mostRecentSnapshotTime = currBlockStart
					}
					existing[shard] = mostRecentSnapshotTime
					mostRecentSnapshotsByBlockShard[currBlockUnixNanos] = existing
				}()

				snapshotFiles, ok := snapshotFilesByShard[shard]
				if !ok {
					// If there are no snapshot files for this shard, then rely on
					// the defer to fallback to using the block start time.
					return
				}

				mostRecentSnapshot, ok = snapshotFiles.LatestVolumeForBlock(currBlockStart)
				if !ok {
					// If there are no complete snapshot files for this block, then rely on
					// the defer to fallback to using the block start time.
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
					s.log.
						WithFields(
							xlog.NewField("namespace", mostRecentSnapshot.ID.Namespace),
							xlog.NewField("blockStart", mostRecentSnapshot.ID.BlockStart),
							xlog.NewField("shard", mostRecentSnapshot.ID.Shard),
							xlog.NewField("index", mostRecentSnapshot.ID.VolumeIndex),
							xlog.NewField("filepaths", mostRecentSnapshot.AbsoluteFilepaths),
						).
						Error("error resolving snapshot time for snapshot file")

					// If we couldn't determine the snapshot time for the snapshot file, then rely on
					// the defer to fallback to using the block start time.
					return
				}

			}()
		}
	}

	return mostRecentSnapshotsByBlockShard
}

func (s *commitLogSource) minimumMostRecentSnapshotTimeByBlock(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	mostRecentSnapshotByBlockShard map[xtime.UnixNano]map[uint32]time.Time,
) map[xtime.UnixNano]time.Time {
	minimumMostRecentSnapshotTimeByBlock := map[xtime.UnixNano]time.Time{}
	for blockStart, mostRecentSnapshotsByShard := range mostRecentSnapshotByBlockShard {

		var minMostRecentSnapshot time.Time
		for shard, mostRecentSnapshotForShard := range mostRecentSnapshotsByShard {
			blockRange := xtime.Range{Start: blockStart.ToTime(), End: blockStart.ToTime().Add(blockSize)}
			if !shardsTimeRanges[shard].Overlaps(blockRange) {
				// In order for a minimum most recent snapshot to be valid, it needs to be for a block that
				// we actually need to bootstrap for that. This check ensures that our algorithm doesn't do any
				// extra work, even if we're bootstrapping different blocks for various shards.
				continue
			}

			if mostRecentSnapshotForShard.Before(minMostRecentSnapshot) || minMostRecentSnapshot.IsZero() {
				minMostRecentSnapshot = mostRecentSnapshotForShard
			}
		}

		if minMostRecentSnapshot.IsZero() {
			// If we didn't find a minimum most recent snapshot time for this blockStart, just use the
			// blockStart as the minimum since we'll need to read the entire commit log in this case.
			minMostRecentSnapshot = blockStart.ToTime()
		}

		minimumMostRecentSnapshotTimeByBlock[blockStart] = minMostRecentSnapshot
	}

	return minimumMostRecentSnapshotTimeByBlock
}

func (s *commitLogSource) bootstrapAvailableSnapshotFiles(
	nsID ident.ID,
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
	fsOpts fs.Options,
	bytesPool pool.CheckedBytesPool,
	blocksPool block.DatabaseBlockPool,
) (map[uint32]result.ShardResult, error) {
	snapshotShardResults := make(map[uint32]result.ShardResult)

	for shard, tr := range shardsTimeRanges {
		rangeIter := tr.Iter()
		for hasMore := rangeIter.Next(); hasMore; hasMore = rangeIter.Next() {
			var (
				currRange             = rangeIter.Value()
				currRangeDuration     = currRange.End.Unix() - currRange.Start.Unix()
				isMultipleOfBlockSize = currRangeDuration/int64(blockSize) == 0
			)

			if !isMultipleOfBlockSize {
				return nil, fmt.Errorf(
					"received bootstrap range that is not multiple of blockSize, blockSize: %d, start: %d, end: %d",
					blockSize, currRange.End.Unix(), currRange.Start.Unix(),
				)
			}

			// TODO: Make function for this iteration?
			// TODO: Estimate capacity better
			shardResult := result.NewShardResult(0, s.opts.ResultOptions())
			for blockStart := currRange.Start.Truncate(blockSize); blockStart.Before(currRange.End); blockStart = blockStart.Add(blockSize) {
				snapshotFiles := snapshotFilesByShard[shard]

				// TODO: Already called this FN, maybe should just re-use the results somehow
				latestSnapshot, ok := snapshotFiles.LatestVolumeForBlock(blockStart)
				if !ok {
					// There are no snapshot files for this shard / block combination
					// TODO: This is sketch, it should just try and read the exact same ones
					// we determined in earlier steps and error out if it cant read them because
					// then the commit log logic we chose is wrong.
					continue
				}

				// Bootstrap the snapshot file
				reader, err := s.newReaderFn(bytesPool, fsOpts)
				if err != nil {
					// TODO: In this case, we want to emit an error log, and somehow propagate that
					// we were unable to read this snapshot file to the subsequent code which determines
					// how much commitlog to read. We might even want to try and read the next earlier file
					// if it exists.
					// Actually, since the commit log file no longer exists, we might just want to mark this
					// as unfulfilled somehow and get on with it.
					return nil, err
				}

				err = reader.Open(fs.DataReaderOpenOptions{
					Identifier: fs.FileSetFileIdentifier{
						Namespace:   nsID,
						BlockStart:  blockStart,
						Shard:       shard,
						VolumeIndex: latestSnapshot.ID.VolumeIndex,
					},
					FileSetType: persist.FileSetSnapshotType,
				})
				if err != nil {
					// TODO: Same comment as above
					return nil, err
				}

				for {
					// TODO: Verify checksum
					id, tagsIter, data, _, err := reader.Read()
					if err != nil && err != io.EOF {
						return nil, err
					}

					if err == io.EOF {
						break
					}

					var tags ident.Tags
					entry, exists := shardResult.AllSeries().Get(id)
					if exists {
						// NB(r): In the case the series is already inserted
						// we can avoid holding onto this ID and use the already
						// allocated ID.
						id.Finalize()
						id = entry.ID
						tags = entry.Tags
					} else {
						// TODO: Optimize this so we don't waste a bunch of time here
						// even when the index is off
						tags, err = s.tagsFromTagsIter(id, tagsIter)
						if err != nil {
							return nil, fmt.Errorf("unable to decode tags: %v", err)
						}
					}
					tagsIter.Close()

					dbBlock := blocksPool.Get()
					dbBlock.Reset(blockStart, blockSize, ts.NewSegment(data, nil, ts.FinalizeHead))

					shardResult.AddBlock(id, tags, dbBlock)
				}
			}
			snapshotShardResults[shard] = shardResult
		}
	}

	return snapshotShardResults, nil
}

func (s *commitLogSource) startM3TSZEncodingWorker(
	ns namespace.Metadata,
	runOpts bootstrap.RunOptions,
	workerNum int,
	ec <-chan encoderArg,
	unmerged []shardData,
	encoderPool encoding.EncoderPool,
	workerErrs []int,
	blopts block.Options,
	wg *sync.WaitGroup,
) {
	shouldCacheSeriesMetadata := s.shouldCacheSeriesMetadata(runOpts, ns)
	for arg := range ec {
		var (
			series     = arg.series
			dp         = arg.dp
			unit       = arg.unit
			annotation = arg.annotation
			blockStart = arg.blockStart
		)

		unmergedShard := unmerged[series.Shard].series
		unmergedSeries, ok := unmergedShard[series.UniqueIndex]
		if !ok {
			if shouldCacheSeriesMetadata {
				// If we're going to cache the IDs and Tags on the commitlog source, then
				// we need to make sure that they won't get finalized by anything else in
				// the code base. Specifically, since series.Tags is a struct (not a pointer
				// to a struct), we need to call NoFinalize() on it as early in the code-path
				// as possible so that the NoFinalize() state is propagated everywhere (since
				// the struct will get copied repeatedly.)
				//
				// This is also the "ideal" spot to mark the IDs as NoFinalize(), because it
				// only occurs once per series per run. So if we end up allocating the IDs/Tags
				// multiple times during the bootstrap, we'll only mark the first appearance as
				// NoFinalize, and all subsequent occurrences can be finalized per usual.
				series.ID.NoFinalize()
				series.Tags.NoFinalize()
			}

			unmergedSeries = metadataAndEncodersByTime{
				id:       series.ID,
				tags:     series.Tags,
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

func (s *commitLogSource) shouldEncodeForData(
	unmerged []shardData,
	dataBlockSize time.Duration,
	series commitlog.Series,
	timestamp time.Time,
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
	blockStart := timestamp.Truncate(dataBlockSize)
	blockEnd := blockStart.Add(dataBlockSize)
	blockRange := xtime.Range{
		Start: blockStart,
		End:   blockEnd,
	}

	return ranges.Overlaps(blockRange)
}

func (s *commitLogSource) shouldIncludeInIndex(
	shard uint32,
	ts time.Time,
	highestShard uint32,
	indexBlockSize time.Duration,
	bootstrapRangesByShard []xtime.Ranges,
) bool {
	if shard > highestShard {
		// Not trying to bootstrap this shard
		return false
	}

	rangesToBootstrap := bootstrapRangesByShard[shard]
	if rangesToBootstrap.IsEmpty() {
		// No ShardTimeRanges were provided for this shard, so we're not
		// bootstrapping it.
		return false
	}

	// Check if the timestamp corresponds to one of the index blocks we're
	// trying to bootstrap.
	indexBlockStart := ts.Truncate(indexBlockSize)
	indexBlockEnd := indexBlockStart.Add(indexBlockSize)
	indexBlockRange := xtime.Range{
		Start: indexBlockStart,
		End:   indexBlockEnd,
	}

	return rangesToBootstrap.Overlaps(indexBlockRange)
}

func (s *commitLogSource) mergeShards(
	numShards int,
	bopts result.Options,
	blockSize time.Duration,
	blopts block.Options,
	encoderPool encoding.EncoderPool,
	unmerged []shardData,
) result.DataBootstrapResult {
	var (
		shardErrs               = make([]int, numShards)
		shardEmptyErrs          = make([]int, numShards)
		bootstrapResult         = result.NewDataBootstrapResult()
		blocksPool              = bopts.DatabaseBlockOptions().DatabaseBlockPool()
		multiReaderIteratorPool = blopts.MultiReaderIteratorPool()
		// Controls how many shards can be merged in parallel
		workerPool          = xsync.NewWorkerPool(s.opts.MergeShardsConcurrency())
		bootstrapResultLock sync.Mutex
		wg                  sync.WaitGroup
	)

	workerPool.Init()

	for shard, unmergedShard := range unmerged {
		if unmergedShard.series == nil {
			continue
		}
		wg.Add(1)
		shard, unmergedShard := shard, unmergedShard
		mergeShardFunc := func() {
			var shardResult result.ShardResult
			shardResult, shardEmptyErrs[shard], shardErrs[shard] = s.mergeShard(
				shard, unmergedShard, blocksPool, multiReaderIteratorPool, encoderPool, blockSize, blopts)

			if shardResult != nil && shardResult.NumSeries() > 0 {
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
	shard int,
	unmergedShard shardData,
	blocksPool block.DatabaseBlockPool,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
	encoderPool encoding.EncoderPool,
	blockSize time.Duration,
	blopts block.Options,
) (result.ShardResult, int, int) {
	var shardResult result.ShardResult
	var numShardEmptyErrs int
	var numErrs int

	for _, unmergedBlocks := range unmergedShard.series {
		seriesBlocks, numSeriesEmptyErrs, numSeriesErrs := s.mergeSeries(
			unmergedBlocks,
			blocksPool,
			multiReaderIteratorPool,
			encoderPool,
			blockSize,
			blopts,
		)

		if seriesBlocks != nil && seriesBlocks.Len() > 0 {
			if shardResult == nil {
				shardResult = result.NewShardResult(len(unmergedShard.series), s.opts.ResultOptions())
			}
			shardResult.AddSeries(unmergedBlocks.id, unmergedBlocks.tags, seriesBlocks)
		}

		numShardEmptyErrs += numSeriesEmptyErrs
		numErrs += numSeriesErrs
	}
	return shardResult, numShardEmptyErrs, numErrs
}

func (s *commitLogSource) mergeSeries(
	unmergedBlocks metadataAndEncodersByTime, blocksPool block.DatabaseBlockPool,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
	encoderPool encoding.EncoderPool,
	blockSize time.Duration,
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
			pooledBlock.Reset(start, blockSize, encoders[0].enc.Discard())
			if seriesBlocks == nil {
				seriesBlocks = block.NewDatabaseSeriesBlocks(len(unmergedBlocks.encoders))
			}
			seriesBlocks.AddBlock(pooledBlock)
			continue
		}

		// Convert encoders to readers so we can use iteration helpers
		readers := encoders.newReaders()
		iter := multiReaderIteratorPool.Get()
		iter.Reset(readers, time.Time{}, 0)

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
		pooledBlock.Reset(start, blockSize, enc.Discard())
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

// cacheShardData caches the shardData from a call to ReadData() on the source so that subsequent calls
// to ReadIndex() for the same time period don't have to read the commit log files again.
//
// In order for the subsequent call to ReadIndex() to avoid reading the same commit log files, we need
// to cache three pieces of information for every series:
// 		1) The ID (so it can be indexed)
// 		2) The tags (so they can be indexed)
// 		3) The block starts for which the series had a datapoint (so that we know which index blocks
//         / segments the series needs to be included in)
//
// In addition, for each shard we will need to store the ranges which we have already read commit log
// files for, so that the ReadIndex() call can easily filter commit log files down to those which
// have not already been read by a previous call to ReadData().
//
// Its safe to cache the series IDs and Tags because we mark them both as NoFinalize() if the caching
// path is enabled.
func (s *commitLogSource) cacheShardData(ns namespace.Metadata, allShardData []shardData) {
	nsString := ns.ID().String()
	nsCache, ok := s.cachedShardDataByNS[nsString]
	if !ok {
		nsShardData := &cachedShardData{
			shardData: make([]shardData, len(allShardData)),
		}
		s.cachedShardDataByNS[nsString] = nsShardData
		nsCache = nsShardData
	}

	for shard, currShardData := range allShardData {
		for _, seriesData := range currShardData.series {
			for blockStart := range seriesData.encoders {
				// Nil out any references to the encoders (which should be closed already anyways),
				// so that they can be GC'd.
				seriesData.encoders[blockStart] = nil
			}
		}

		for shard >= len(nsCache.shardData) {
			// Extend the slice if necessary (could happen if different calls to
			// ReadData() bootstrap different shards.)
			nsCache.shardData = append(nsCache.shardData, shardData{})
		}

		nsCache.shardData[shard].ranges = nsCache.shardData[shard].ranges.AddRanges(currShardData.ranges)

		currSeries := currShardData.series
		cachedSeries := nsCache.shardData[shard].series

		// If there are no existing series, just set what we have.
		if len(cachedSeries) == 0 {
			if currSeries != nil {
				nsCache.shardData[shard].series = currSeries
			} else {
				nsCache.shardData[shard].series = make(map[uint64]metadataAndEncodersByTime)
			}
			continue
		}

		// If there are existing series, then add any new series that we have, and merge block starts.
		for uniqueIdx, seriesData := range currSeries {
			// If its not already there, just add it
			cachedSeriesData, ok := cachedSeries[uniqueIdx]
			if ok {
			}
			if !ok {
				cachedSeries[uniqueIdx] = seriesData
				continue
			}

			// If it is there, merge blockStart times
			for blockStart := range seriesData.encoders {
				// The existence of a key in the map is indicative of its presence in this case,
				// so assigning nil is equivalent to adding an item to a set. This is counter-intuitive,
				// but we do it so that we can re-use the existing datastructures that have already been
				// allocated by the bootstrapping process, otherwise we'd have to perform millions of
				// additional allocations.
				cachedSeriesData.encoders[blockStart] = nil
			}
		}
	}
}

func (s *commitLogSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Commit log bootstrapper is a last ditch effort, so fulfill all
	// time ranges requested even if not enough data, just to succeed
	// the bootstrap
	return shardsTimeRanges
}

func (s *commitLogSource) ReadIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (result.IndexBootstrapResult, error) {
	if !ns.Options().IndexOptions().Enabled() {
		return result.NewIndexBootstrapResult(), errIndexingNotEnableForNamespace
	}

	if shardsTimeRanges.IsEmpty() {
		return result.NewIndexBootstrapResult(), nil
	}

	var (
		nsCache                        = s.cachedShardDataByNS[ns.ID().String()]
		shardsTimeRangesToReadFromDisk = shardsTimeRanges.Copy()
	)
	if nsCache != nil {
		cachedShardsTimeRanges := result.ShardTimeRanges{}
		for shard, shardData := range nsCache.shardData {
			cachedShardsTimeRanges[uint32(shard)] = shardData.ranges
		}
		shardsTimeRangesToReadFromDisk.Subtract(cachedShardsTimeRanges)
	}

	// Setup predicates for skipping files / series at iterator and reader level.
	readCommitLogPredicate := newReadCommitLogPredicate(
		ns, shardsTimeRangesToReadFromDisk, s.opts, s.inspection)
	readSeriesPredicate := newReadSeriesPredicate(ns)
	iterOpts := commitlog.IteratorOpts{
		CommitLogOptions:      s.opts.CommitLogOptions(),
		FileFilterPredicate:   readCommitLogPredicate,
		SeriesFilterPredicate: readSeriesPredicate,
	}

	// Create the commitlog iterator
	iter, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}
	defer iter.Close()

	highestShard := s.findHighestShard(shardsTimeRanges)
	// +1 so we can use the shard number as an index throughout without constantly
	// remembering to subtract 1 to convert to zero-based indexing.
	numShards := highestShard + 1
	// Convert the map to a slice for faster lookups
	bootstrapRangesByShard := make([]xtime.Ranges, numShards)
	for shard, ranges := range shardsTimeRanges {
		bootstrapRangesByShard[shard] = ranges
	}

	var (
		indexResult    = result.NewIndexBootstrapResult()
		indexResults   = indexResult.IndexResults()
		indexOptions   = ns.Options().IndexOptions()
		indexBlockSize = indexOptions.BlockSize()
		resultOptions  = s.opts.ResultOptions()
	)

	// Start by reading all the commit log files that we couldn't eliminate due to the
	// cached metadata.
	for iter.Next() {
		series, dp, _, _ := iter.Current()

		s.maybeAddToIndex(
			series.ID, series.Tags, series.Shard, highestShard, dp.Timestamp, bootstrapRangesByShard,
			indexResults, indexOptions, indexBlockSize, resultOptions)
	}

	// Add in all the data that was cached by a previous run of ReadData() (if any).
	if nsCache != nil {
		for shard, shardData := range nsCache.shardData {
			for _, series := range shardData.series {
				for dataBlockStart := range series.encoders {
					s.maybeAddToIndex(
						series.id, series.tags, uint32(shard), highestShard, dataBlockStart.ToTime(), bootstrapRangesByShard,
						indexResults, indexOptions, indexBlockSize, resultOptions)
				}
			}
		}
	}

	// If all successful then we mark each index block as fulfilled
	for _, block := range indexResult.IndexResults() {
		blockRange := xtime.Range{
			Start: block.BlockStart(),
			End:   block.BlockStart().Add(indexOptions.BlockSize()),
		}
		fulfilled := result.ShardTimeRanges{}
		for shard, timeRanges := range shardsTimeRanges {
			iter := timeRanges.Iter()
			for iter.Next() {
				curr := iter.Value()
				intersection, intersects := curr.Intersect(blockRange)
				if intersects {
					fulfilled[shard] = fulfilled[shard].AddRange(intersection)
				}
			}
		}
		// Now mark as much of the block that we fulfilled
		err := indexResult.IndexResults().MarkFulfilled(blockRange.Start,
			fulfilled, indexOptions)
		if err != nil {
			return nil, err
		}
	}

	return indexResult, nil
}

func (s commitLogSource) maybeAddToIndex(
	id ident.ID,
	tags ident.Tags,
	shard uint32,
	highestShard uint32,
	blockStart time.Time,
	bootstrapRangesByShard []xtime.Ranges,
	indexResults result.IndexResults,
	indexOptions namespace.IndexOptions,
	indexBlockSize time.Duration,
	resultOptions result.Options,
) error {
	if !s.shouldIncludeInIndex(
		shard, blockStart, highestShard, indexBlockSize, bootstrapRangesByShard) {
		return nil
	}

	segment, err := indexResults.GetOrAddSegment(blockStart, indexOptions, resultOptions)
	if err != nil {
		return err
	}

	exists, err := segment.ContainsID(id.Bytes())
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	// We can use the NoClone variant here because the cached IDs/Tags are marked NoFinalize
	// by the ReadData() path when it reads from the commitlog files, and the IDs/Tags read
	// from the commit log files by the ReadIndex() method won't be finalized because this
	// code path doesn't finalize them.
	d, err := convert.FromMetricNoClone(id, tags)
	if err != nil {
		return err
	}

	_, err = segment.Insert(d)
	return err
}

func (s commitLogSource) shouldCacheSeriesMetadata(runOpts bootstrap.RunOptions, nsMeta namespace.Metadata) bool {
	return runOpts.CacheSeriesMetadata() && nsMeta.Options().IndexOptions().Enabled()
}

// TODO: Share this with the fs source somehow
func (s commitLogSource) tagsFromTagsIter(
	seriesID ident.ID,
	iter ident.TagIterator,
) (ident.Tags, error) {
	var (
		seriesIDBytes = ident.BytesID(seriesID.Bytes())
		// TODO: Don't call all these functions here
		idPool = s.opts.CommitLogOptions().IdentifierPool()
		tags   = idPool.Tags()
	)

	for iter.Next() {
		curr := iter.Current()

		var (
			nameBytes, valueBytes = curr.Name.Bytes(), curr.Value.Bytes()
			tag                   ident.Tag
			idRef                 bool
		)
		if idx := bytes.Index(seriesIDBytes, nameBytes); idx != -1 {
			tag.Name = seriesIDBytes[idx : idx+len(nameBytes)]
			idRef = true
		} else {
			tag.Name = idPool.Clone(curr.Name)
		}
		if idx := bytes.Index(seriesIDBytes, valueBytes); idx != -1 {
			tag.Value = seriesIDBytes[idx : idx+len(valueBytes)]
			idRef = true
		} else {
			tag.Value = idPool.Clone(curr.Value)
		}

		if idRef {
			tag.NoFinalize() // Taken ref, cannot finalize this
		}

		tags.Append(tag)
	}
	return tags, iter.Err()
}

func newReadCommitLogPredicate(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts Options,
	inspection fs.Inspection,
) commitlog.FileFilterPredicate {
	// Minimum and maximum times for which we want to bootstrap
	shardMin, shardMax := shardsTimeRanges.MinMax()
	shardRange := xtime.Range{
		Start: shardMin,
		End:   shardMax,
	}

	// How far into the past or future a commitlog might contain a write for a
	// previous or future block
	bufferPast := ns.Options().RetentionOptions().BufferPast()
	bufferFuture := ns.Options().RetentionOptions().BufferFuture()

	// commitlogFilesPresentBeforeStart is a set of all the commitlog files that were
	// on disk before the node started.
	commitlogFilesPresentBeforeStart := inspection.CommitLogFilesSet()

	return func(name string, entryTime time.Time, entryDuration time.Duration) bool {
		_, ok := commitlogFilesPresentBeforeStart[name]
		if !ok {
			// If the file wasn't on disk before the node started then it only contains
			// writes that are already in memory (and in-fact the file may be actively
			// being written to.)
			return false
		}

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

type shardData struct {
	series map[uint64]metadataAndEncodersByTime
	ranges xtime.Ranges
}

type metadataAndEncodersByTime struct {
	id   ident.ID
	tags ident.Tags
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

type ioReaders []xio.SegmentReader

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

type cachedShardData struct {
	shardData []shardData
}
