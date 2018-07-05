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
		snapshotTimeFn:  fs.SnapshotTime,
		newReaderFn:     fs.NewReader,

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

// ReadData will read a combination of the available snapshot files and commit log files to
// restore as much unflushed data from disk as possible. The logic for performing this
// correctly is as follows:
//
//    1. 	For every shard/blockStart combination, find the most recently written and complete
//        (has a checkpoint file) snapshot. Bootstrap that file.
//    2. 	For every shard/blockStart combination, determine the SnapshotTime for the snapshot file.
//        This value corresponds to the (local) moment in time right before the snapshotting process
//        began.
//    3. 	Find the minimum SnapshotTime for all of the shards and block starts (call it t0), and
//        replay all commit log entries whose system timestamps overlap the range
//        [minimumSnapshotTimeAcrossShards, blockStart.Add(blockSize).Add(bufferPast)]. This logic
//        has one exception which is in the case where there is no minimimum snapshot time across
//        shards (the code treats this case as minimum snapshot time across shards == blockStart).
//        In that case, we replay all commit log entries whose system timestamps overlap the range
//        [blockStart.Add(-bufferFuture), blockStart.Add(blockSize).Add(bufferPast)]
func (s *commitLogSource) ReadData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	var (
		fsOpts         = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix = fsOpts.FilePathPrefix()
	)

	// Determine which snapshot files are available.
	snapshotFilesByShard, err := s.snapshotFilesByShard(
		ns.ID(), filePathPrefix, shardsTimeRanges)
	if err != nil {
		return nil, err
	}

	var (
		bOpts     = s.opts.ResultOptions()
		blOpts    = bOpts.DatabaseBlockOptions()
		blockSize = ns.Options().RetentionOptions().BlockSize()
	)

	// Determine the minimum number of commit logs files that we
	// must read based on the available snapshot files.
	readCommitLogPred, err := s.newReadCommitLogPredBasedOnAvailableSnapshotFiles(
		ns, shardsTimeRanges, snapshotFilesByShard)
	if err != nil {
		return nil, err
	}

	var (
		nsID              = ns.ID()
		seriesSkipped     int
		datapointsSkipped int
		datapointsRead    int

		// TODO(rartoul): When we implement caching data across namespaces, this will need
		// to be commitlog.ReadAllSeriesPredicate() if CacheSeriesMetadata() is enabled
		// because we'll need to read data for all namespaces, not just the one we're currently
		// bootstrapping.
		readSeriesPredicate = func(id ident.ID, namespace ident.ID) bool {
			shouldReadSeries := nsID.Equal(namespace)
			if !shouldReadSeries {
				seriesSkipped++
			}
			return shouldReadSeries
		}

		iterOpts = commitlog.IteratorOpts{
			CommitLogOptions:      s.opts.CommitLogOptions(),
			FileFilterPredicate:   readCommitLogPred,
			SeriesFilterPredicate: readSeriesPredicate,
		}
	)

	defer func() {
		s.log.Infof("seriesSkipped: %d", seriesSkipped)
		s.log.Infof("datapointsSkipped: %d", datapointsSkipped)
		s.log.Infof("datapointsRead: %d", datapointsRead)
	}()

	iter, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}

	defer iter.Close()

	var (
		// +1 so we can use the shard number as an index throughout without constantly
		// remembering to subtract 1 to convert to zero-based indexing
		numShards        = s.findHighestShard(shardsTimeRanges) + 1
		numConc          = s.opts.EncodingConcurrency()
		encoderPool      = blOpts.EncoderPool()
		workerErrs       = make([]int, numConc)
		shardDataByShard = s.newShardDataByShard(shardsTimeRanges, numShards)
	)

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
			datapointsSkipped++
			continue
		}

		datapointsRead++

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

	if iterErr := iter.Err(); iterErr != nil {
		return nil, iterErr
	}

	for _, encoderChan := range encoderChans {
		close(encoderChan)
	}

	// Block until all data has been read and encoded by the worker goroutines
	wg.Wait()
	s.logEncodingOutcome(workerErrs, iter)

	mergeStart := time.Now()
	s.log.Infof("Starting merge...")
	bootstrapResult := s.mergeShards(
		ns,
		shardsTimeRanges,
		snapshotFilesByShard,
		int(numShards),
		blockSize,
		shardDataByShard,
	)
	s.log.Infof("Done merging..., took: %s", time.Now().Sub(mergeStart).String())

	// After merging shards, its safe to cache the shardData (which involves some mutation).
	if s.shouldCacheSeriesMetadata(runOpts, ns) {
		s.cacheShardData(ns, shardDataByShard)
	}

	return bootstrapResult, nil
}

func (s *commitLogSource) snapshotFilesByShard(
	nsID ident.ID,
	filePathPrefix string,
	shardsTimeRanges result.ShardTimeRanges,
) (map[uint32]fs.FileSetFilesSlice, error) {
	snapshotFilesByShard := map[uint32]fs.FileSetFilesSlice{}
	for shard := range shardsTimeRanges {
		snapshotFiles, err := s.snapshotFilesFn(filePathPrefix, nsID, shard)
		if err != nil {
			return nil, err
		}
		snapshotFilesByShard[shard] = snapshotFiles
	}

	return snapshotFilesByShard, nil
}

func (s *commitLogSource) newShardDataByShard(
	shardsTimeRanges result.ShardTimeRanges,
	numShards uint32,
) []shardData {
	shardDataByShard := make([]shardData, numShards)
	for shard := range shardsTimeRanges {
		shardDataByShard[shard] = shardData{
			series: make(map[uint64]metadataAndEncodersByTime),
			ranges: shardsTimeRanges[shard],
		}
	}

	return shardDataByShard
}

func (s *commitLogSource) mostRecentCompleteSnapshotTimeByBlockShard(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
	fsOpts fs.Options,
) map[xtime.UnixNano]map[uint32]time.Time {
	var (
		minBlock, maxBlock              = shardsTimeRanges.MinMax()
		decoder                         = msgpack.NewDecoder(nil)
		mostRecentSnapshotsByBlockShard = map[xtime.UnixNano]map[uint32]time.Time{}
	)

	for currBlockStart := minBlock.Truncate(blockSize); currBlockStart.Before(maxBlock); currBlockStart = currBlockStart.Add(blockSize) {
		for shard := range shardsTimeRanges {
			// Anonymous func for easier clean up using defer.
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

				// Performs I/O.
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
				// we actually need to bootstrap for that shard. This check ensures that our algorithm doesn't
				// do any extra work, even if we're bootstrapping different blocks for various shards.
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
) (map[uint32]result.ShardResult, error) {
	snapshotShardResults := make(map[uint32]result.ShardResult)

	for shard, tr := range shardsTimeRanges {
		shardResult, err := s.bootstrapShardSnapshots(
			nsID, shard, tr, blockSize, snapshotFilesByShard[shard])
		if err != nil {
			return nil, err
		}
		snapshotShardResults[shard] = shardResult
	}

	return snapshotShardResults, nil
}

func (s *commitLogSource) bootstrapShardSnapshots(
	nsID ident.ID,
	shard uint32,
	shardTimeRanges xtime.Ranges,
	blockSize time.Duration,
	snapshotFiles fs.FileSetFilesSlice,
) (result.ShardResult, error) {
	var (
		shardResult    result.ShardResult
		allSeriesSoFar *result.Map
		rangeIter      = shardTimeRanges.Iter()
		err            error
	)

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

		// Reset this after we bootstrap each block to make sure its up to date.
		if shardResult != nil {
			allSeriesSoFar = shardResult.AllSeries()
		}

		for blockStart := currRange.Start.Truncate(blockSize); blockStart.Before(currRange.End); blockStart = blockStart.Add(blockSize) {
			shardResult, err = s.bootstrapShardBlockSnapshot(
				nsID, shard, blockStart, shardResult, allSeriesSoFar, blockSize, snapshotFiles)
			if err != nil {
				return shardResult, err
			}
		}
	}

	if shardResult == nil {
		shardResult = result.NewShardResult(0, s.opts.ResultOptions())
	}
	return shardResult, nil
}

func (s *commitLogSource) bootstrapShardBlockSnapshot(
	nsID ident.ID,
	shard uint32,
	blockStart time.Time,
	shardResult result.ShardResult,
	allSeriesSoFar *result.Map,
	blockSize time.Duration,
	snapshotFiles fs.FileSetFilesSlice,
) (result.ShardResult, error) {
	var (
		bOpts      = s.opts.ResultOptions()
		blOpts     = bOpts.DatabaseBlockOptions()
		blocksPool = blOpts.DatabaseBlockPool()
		bytesPool  = blOpts.BytesPool()
		fsOpts     = s.opts.CommitLogOptions().FilesystemOptions()
	)

	// TODO: Already called this FN, maybe should just re-use the results somehow
	latestSnapshot, ok := snapshotFiles.LatestVolumeForBlock(blockStart)
	if !ok {
		s.log.Infof(
			"No snapshots for shard: %d and blockStart: %d",
			shard, blockStart.Unix())
		// There are no snapshot files for this shard / block combination
		// TODO: This is sketch, it should just try and read the exact same ones
		// we determined in earlier steps and error out if it cant read them because
		// then the commit log logic we chose is wrong.
		return shardResult, nil
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
		return shardResult, err
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
		return shardResult, err
	}

	s.log.Infof(
		"Reading snapshot for shard: %d and blockStart: %d and volume: %d",
		shard, blockStart.Unix(), latestSnapshot.ID.VolumeIndex)
	for {
		id, tagsIter, data, expectedChecksum, err := reader.Read()
		if err != nil && err != io.EOF {
			return shardResult, err
		}

		if err == io.EOF {
			break
		}

		// TODO: Optimize this so we don't waste a bunch of time here
		// even when the index is disabled
		tags, err := s.tagsFromTagsIter(id, tagsIter)
		if err != nil {
			return shardResult, fmt.Errorf("unable to decode tags: %v", err)
		}
		tagsIter.Close()

		dbBlock := blocksPool.Get()
		dbBlock.Reset(blockStart, blockSize, ts.NewSegment(data, nil, ts.FinalizeHead))
		// Resetting the block will trigger a checksum calculation, so use that instead
		// of calculating it twice.
		checksum, err := dbBlock.Checksum()
		if err != nil {
			return shardResult, err
		}

		if checksum != expectedChecksum {
			// TODO: Need to propagate back better
			return shardResult, fmt.Errorf("checksum for series: %s was %d but expected %d", id, checksum, expectedChecksum)
		}

		if allSeriesSoFar != nil {
			existing, ok := allSeriesSoFar.Get(id)
			if ok {
				// If we've already bootstrapped this series for a different block, we don't need
				// another copy of the IDs and tags.
				// TODO: Make sure this is right.
				id.Finalize()
				tags.Finalize()
				id = existing.ID
				tags = existing.Tags
			}
		}

		// TODO: In the exists case we can probably optimize this to add
		// directly to existing to avoid an extra map lookup.
		if shardResult == nil {
			// Delay initialization so we can estimate size.
			shardResult = result.NewShardResult(reader.Entries(), s.opts.ResultOptions())
		}
		shardResult.AddBlock(id, tags, dbBlock)
	}

	return shardResult, nil
}

func (s *commitLogSource) newReadCommitLogPredBasedOnAvailableSnapshotFiles(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
) (func(fileName string, fileStart time.Time, fileBlockSize time.Duration) bool, error) {
	blockSize := ns.Options().RetentionOptions().BlockSize()

	// At this point we've bootstrapped all the snapshot files that we can, and we need to
	// decide which commit logs to read. In order to do that, we'll need to figure out the
	// minimum most recent snapshot time for each block, then we can use that information to
	// decide how much of the commit log we need to read for each block that we're bootstrapping.
	// To start, for each block that we're bootstrapping, we need to figure out the most recent
	// snapshot that was taken for each shard. I.E we want to create a datastructure that looks
	// like this:
	// 		map[blockStart]map[shard]mostRecentSnapshotTime
	mostRecentCompleteSnapshotTimeByBlockShard := s.mostRecentCompleteSnapshotTimeByBlockShard(
		shardsTimeRanges, blockSize, snapshotFilesByShard, s.opts.CommitLogOptions().FilesystemOptions())
	for block, mostRecentByShard := range mostRecentCompleteSnapshotTimeByBlockShard {
		for shard, mostRecent := range mostRecentByShard {
			s.log.Infof(
				"Most recent snapshot for block: %d and shard: %d is %d",
				block.ToTime().Unix(), shard, mostRecent.Unix())
		}
	}

	// Once we have the desired data structure, we next need to figure out the minimum most recent
	// snapshot for that block across all shards. This will help us determine how much of the commit
	// log we need to read. The new data structure we're trying to generate looks like:
	// 		map[blockStart]minimumMostRecentSnapshotTime (across all shards)
	// This structure is important because it tells us how much of the commit log we need to read for
	// each block that we're trying to bootstrap (because the commit log is shared across all shards.)
	minimumMostRecentSnapshotTimeByBlock := s.minimumMostRecentSnapshotTimeByBlock(
		shardsTimeRanges, blockSize, mostRecentCompleteSnapshotTimeByBlockShard)
	for block, minSnapshotTime := range minimumMostRecentSnapshotTimeByBlock {
		s.log.Infof(
			"Min snapshot time for block: %d is: %d",
			block.ToTime().Unix(), minSnapshotTime.Unix())
	}

	// Now that we have the minimum most recent snapshot time for each block, we can use that data to
	// decide how much of the commit log we need to read for each block that we're bootstrapping. We'll
	// construct a new predicate based on the data structure we constructed earlier where the new
	// predicate will check if there is any overlap between a commit log file and a temporary range
	// we construct that begins with the minimum snapshot time and ends with the end of that block + bufferPast.
	return s.newReadCommitLogPred(ns, minimumMostRecentSnapshotTimeByBlock), nil
}

func (s *commitLogSource) newReadCommitLogPred(
	ns namespace.Metadata,
	minimumMostRecentSnapshotTimeByBlock map[xtime.UnixNano]time.Time,
) func(fileName string, fileStart time.Time, fileBlockSize time.Duration) bool {
	var (
		rOpts                            = ns.Options().RetentionOptions()
		blockSize                        = rOpts.BlockSize()
		bufferPast                       = rOpts.BufferPast()
		bufferFuture                     = rOpts.BufferFuture()
		rangesToCheck                    = []xtime.Range{}
		commitlogFilesPresentBeforeStart = s.inspection.CommitLogFilesSet()
	)

	for blockStart, minimumMostRecentSnapshotTime := range minimumMostRecentSnapshotTimeByBlock {
		// blockStart.Add(blockSize) represents the logical range that we're trying to bootstrap, but
		// commitlog and snapshot timestamps are system timestamps so we need to create a system
		// time range against which we can compare our commit log ranges.
		//
		// In this case, the snapshot will contain all datapoints for a given block that were received/written
		// (system time) before the snapshot time, so we use that as the start of our range.
		//
		// The end of our range is the end of the block + the bufferPast window. This is because its
		// still possible for writes for the block that we're trying to bootstrap to arrive up until
		// blockStart.Add(blockSize).Add(bufferPast).
		//
		// Note that in the general case (snapshot files are present) we don't need to check bufferFuture
		// at all, because the snapshot is guaranteed to have all writes that were written before the
		// snapshot time, which includes any datapoints written during the bufferFuture range, by definition.
		// However, if there is no snapshot (minimumMostRecentSnapshotTime.Equal(blockStart)), then we DO
		// have to take bufferFuture into account because commit logs with system timestamps in the previous
		// block may contain writes for the block that we're trying to bootstrap, and we can't rely upon the
		// fact that they are already included in our (non-existent) snapshot.
		if minimumMostRecentSnapshotTime.Equal(blockStart.ToTime()) {
			minimumMostRecentSnapshotTime = minimumMostRecentSnapshotTime.Add(-bufferFuture)
		}
		rangesToCheck = append(rangesToCheck, xtime.Range{
			Start: minimumMostRecentSnapshotTime,
			End:   blockStart.ToTime().Add(blockSize).Add(bufferPast),
		})
	}

	// TODO: We have to rely on the global minimum across shards to determine which commit log files
	// we need to read, but we can still skip datapoints from the commitlog itself that belong to a shard
	// that has a snapshot more recent than the global minimum. If we use an array for fast-access this could
	// be a net win.
	return func(fileName string, fileStart time.Time, fileBlockSize time.Duration) bool {
		_, ok := commitlogFilesPresentBeforeStart[fileName]
		if !ok {
			// If the file wasn't on disk before the node started then it only contains
			// writes that are already in memory (and in-fact the file may be actively
			// being written to.)
			return false
		}

		for _, rangeToCheck := range rangesToCheck {
			commitLogEntryRange := xtime.Range{
				Start: fileStart,
				End:   fileStart.Add(fileBlockSize),
			}

			if commitLogEntryRange.Overlaps(rangeToCheck) {
				s.log.
					Infof(
						"Opting to read commit log: %s with start: %d and duration: %s",
						fileName, fileStart.Unix(), fileBlockSize.String())
				return true
			}
		}

		s.log.
			Infof(
				"Opting to skip commit log: %s with start: %d and duration: %s",
				fileName, fileStart.Unix(), fileBlockSize.String())
		return false
	}
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
				encoders: make(map[xtime.UnixNano][]encoder)}
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
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	snapshotFiles map[uint32]fs.FileSetFilesSlice,
	numShards int,
	blockSize time.Duration,
	unmerged []shardData,
) result.DataBootstrapResult {
	var (
		shardErrs       = make([]int, numShards)
		shardEmptyErrs  = make([]int, numShards)
		bootstrapResult = result.NewDataBootstrapResult()
		// Controls how many shards can be merged in parallel
		workerPool          = xsync.NewWorkerPool(s.opts.MergeShardsConcurrency())
		bootstrapResultLock sync.Mutex
		wg                  sync.WaitGroup
	)
	workerPool.Init()

	for shard, unmergedShard := range unmerged {
		snapshotData, err := s.bootstrapShardSnapshots(
			ns.ID(),
			uint32(shard),
			shardsTimeRanges[uint32(shard)],
			blockSize,
			snapshotFiles[uint32(shard)],
		)
		if err != nil {
			panic(err)
		}

		if unmergedShard.series == nil {
			if !snapshotData.IsEmpty() {
				// No snapshot or commit log data, skip.
				continue
			}

			// No commit log data, but we do have snapshot data, so just use that.
			bootstrapResultLock.Lock()
			bootstrapResult.Add(uint32(shard), snapshotData, xtime.Ranges{})
			bootstrapResultLock.Unlock()
		}

		// Have snapshot and commit log data, so we need to merge.
		wg.Add(1)
		shard, unmergedShard := shard, unmergedShard
		mergeShardFunc := func() {
			var shardResult result.ShardResult
			shardResult, shardEmptyErrs[shard], shardErrs[shard] = s.mergeShard(
				shard, snapshotData, unmergedShard, blockSize)

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
	snapshotData result.ShardResult,
	unmergedShard shardData,
	blockSize time.Duration,
) (result.ShardResult, int, int) {
	var (
		bOpts                   = s.opts.ResultOptions()
		blOpts                  = bOpts.DatabaseBlockOptions()
		blocksPool              = blOpts.DatabaseBlockPool()
		multiReaderIteratorPool = blOpts.MultiReaderIteratorPool()
		segmentReaderPool       = blOpts.SegmentReaderPool()
		encoderPool             = blOpts.EncoderPool()
	)

	var shardResult = result.NewShardResult(len(unmergedShard.series), s.opts.ResultOptions())
	var numShardEmptyErrs int
	var numErrs int

	allSnapshotSeries := snapshotData.AllSeries()
	for _, unmergedBlocks := range unmergedShard.series {
		snapshotSeriesData, _ := allSnapshotSeries.Get(unmergedBlocks.id)
		seriesBlocks, numSeriesEmptyErrs, numSeriesErrs := s.mergeSeries(
			snapshotSeriesData,
			unmergedBlocks,
			blocksPool,
			multiReaderIteratorPool,
			segmentReaderPool,
			encoderPool,
			blockSize,
			blOpts,
		)

		if seriesBlocks != nil && seriesBlocks.Len() > 0 {
			shardResult.AddSeries(unmergedBlocks.id, unmergedBlocks.tags, seriesBlocks)
		}

		numShardEmptyErrs += numSeriesEmptyErrs
		numErrs += numSeriesErrs
	}

	allShardResultSeries := shardResult.AllSeries()
	for _, val := range allSnapshotSeries.Iter() {
		id := val.Key()
		blocks := val.Value()

		if allShardResultSeries.Contains(id) {
			// Already merged so we know the ID and tags from the snapshot
			// won't be used and can be closed. We can't close the blocks
			// though because we may have loaded some of the blocks into
			// the shard result and we don't want to close them.
			// TODO: Is this 100% correct?
			id.Finalize()
			blocks.Tags.Finalize()
			continue
		}

		shardResult.AddSeries(id, blocks.Tags, blocks.Blocks)
	}
	return shardResult, numShardEmptyErrs, numErrs
}

func (s *commitLogSource) mergeSeries(
	snapshotData result.DatabaseSeriesBlocks,
	unmergedCommitlogBlocks metadataAndEncodersByTime,
	blocksPool block.DatabaseBlockPool,
	multiReaderIteratorPool encoding.MultiReaderIteratorPool,
	segmentReaderPool xio.SegmentReaderPool,
	encoderPool encoding.EncoderPool,
	blockSize time.Duration,
	blopts block.Options,
) (block.DatabaseSeriesBlocks, int, int) {
	var seriesBlocks block.DatabaseSeriesBlocks
	var numEmptyErrs int
	var numErrs int

	for startNano, encoders := range unmergedCommitlogBlocks.encoders {
		start := startNano.ToTime()

		var (
			snapshotBlock    block.DatabaseBlock
			hasSnapshotBlock bool
		)

		if snapshotData.Blocks != nil {
			snapshotBlock, hasSnapshotBlock = snapshotData.Blocks.BlockAt(start)
		}

		if !hasSnapshotBlock {
			// Make sure snapshotBlock is nil if it does not exist.
			snapshotBlock = nil
		}

		// Closes encoders and snapshotBlock by calling Discard() on each.
		readers, err := newIOReadersFromEncodersAndBlock(
			segmentReaderPool, encoders, snapshotBlock)
		if err != nil {
			panic(err)
		}

		iter := multiReaderIteratorPool.Get()
		iter.Reset(readers, time.Time{}, 0)

		enc := encoderPool.Get()
		enc.Reset(start, blopts.DatabaseBlockAllocSize())
		for iter.Next() {
			dp, unit, annotation := iter.Current()
			encodeErr := enc.Encode(dp, unit, annotation)
			if encodeErr != nil {
				if err != nil {
					panic(err)
				}
				err = encodeErr
				numErrs++
				break
			}
		}

		if iterErr := iter.Err(); iterErr != nil {
			panic(iterErr)
			if err == nil {
				err = iter.Err()
			}
			numErrs++
		}

		// Automatically returns iter to the pool
		iter.Close()
		readers.close()
		if hasSnapshotBlock {
			// Block is already closed, but we need to remove from the Blocks
			// to prevent a double free when we call Blocks.Close() later.
			snapshotData.Blocks.RemoveBlockAt(start)
		}

		if err != nil {
			panic(err)
			continue
		}

		pooledBlock := blocksPool.Get()
		pooledBlock.Reset(start, blockSize, enc.Discard())
		if seriesBlocks == nil {
			seriesBlocks = block.NewDatabaseSeriesBlocks(len(unmergedCommitlogBlocks.encoders))
		}
		seriesBlocks.AddBlock(pooledBlock)
	}

	if snapshotData.Blocks != nil {
		allSnapshotBlocks := snapshotData.Blocks.AllBlocks()
		for startNano, snapshotBlock := range snapshotData.Blocks.AllBlocks() {
			if seriesBlocks == nil {
				seriesBlocks = block.NewDatabaseSeriesBlocks(len(allSnapshotBlocks))
			}
			_, ok := seriesBlocks.BlockAt(startNano.ToTime())
			if ok {
				// Shouldnt happen?
				// Already merged
				continue
			}

			seriesBlocks.AddBlock(snapshotBlock)
		}
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

	var (
		fsOpts         = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix = fsOpts.FilePathPrefix()
	)
	snapshotFilesByShard, err := s.snapshotFilesByShard(
		ns.ID(), filePathPrefix, shardsTimeRanges)
	if err != nil {
		return nil, err
	}

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
		blockSize      = ns.Options().RetentionOptions().BlockSize()
	)

	// TODO: Can optimize this to not read data, and just return the
	// IDs / tags.
	// Start by bootstrapping any available snapshot files.
	snapshotShardResults, err := s.bootstrapAvailableSnapshotFiles(
		ns.ID(), shardsTimeRanges, blockSize, snapshotFilesByShard)
	if err != nil {
		return nil, err
	}

	// Bootstrap any series we got from the snapshot files into the index.
	for shard, result := range snapshotShardResults {
		for _, val := range result.AllSeries().Iter() {
			id := val.Key()
			val := val.Value()
			for block := range val.Blocks.AllBlocks() {
				s.maybeAddToIndex(
					id, val.Tags, shard, highestShard, block.ToTime(), bootstrapRangesByShard,
					indexResults, indexOptions, indexBlockSize, resultOptions)
			}
		}
	}

	// Next, we need to read all data out of the commit log that wasn't covered by the
	// snapshot files or cached metadata (previously read commit logs).
	readCommitLogPredicate, err := s.newReadCommitLogPredBasedOnAvailableSnapshotFiles(
		ns, shardsTimeRanges, snapshotFilesByShard)
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

	for iter.Next() {
		series, dp, _, _ := iter.Current()

		s.maybeAddToIndex(
			series.ID, series.Tags, series.Shard, highestShard, dp.Timestamp, bootstrapRangesByShard,
			indexResults, indexOptions, indexBlockSize, resultOptions)
	}

	// Finally, add in all the data that was cached by a previous run of ReadData() (if any).
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
	encoders map[xtime.UnixNano][]encoder
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

type ioReaders []xio.SegmentReader

func newIOReadersFromEncodersAndBlock(
	segmentReaderPool xio.SegmentReaderPool,
	encoders []encoder,
	dbBlock block.DatabaseBlock,
) (ioReaders, error) {
	numReaders := len(encoders)
	if dbBlock != nil {
		numReaders++
	}

	readers := make(ioReaders, 0, numReaders)
	if dbBlock != nil {
		blockSegment := dbBlock.Discard()
		blockReader := segmentReaderPool.Get()
		blockReader.Reset(blockSegment)
		readers = append(readers, blockReader)
	}

	for _, encoder := range encoders {
		segmentReader := segmentReaderPool.Get()
		segmentReader.Reset(encoder.enc.Discard())
		readers = append(readers, segmentReader)
	}

	return readers, nil
}

func (ir ioReaders) close() {
	for _, r := range ir {
		r.(xio.SegmentReader).Finalize()
	}
}

type cachedShardData struct {
	shardData []shardData
}
