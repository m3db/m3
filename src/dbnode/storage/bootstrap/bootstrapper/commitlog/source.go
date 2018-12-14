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
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xsync "github.com/m3db/m3x/sync"
	xtime "github.com/m3db/m3x/time"

	"github.com/uber-go/tally"
)

var (
	errIndexingNotEnableForNamespace = errors.New("indexing not enabled for namespace")
)

const encoderChanBufSize = 1000

type newIteratorFn func(opts commitlog.IteratorOpts) (
	iter commitlog.Iterator, corruptFiles []commitlog.ErrorWithPath, err error)
type snapshotFilesFn func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error)
type newReaderFn func(bytesPool pool.CheckedBytesPool, opts fs.Options) (fs.DataFileSetReader, error)

type commitLogSource struct {
	opts Options
	log  xlog.Logger

	// Filesystem inspection capture before node was started.
	inspection fs.Inspection

	newIteratorFn   newIteratorFn
	snapshotFilesFn snapshotFilesFn
	newReaderFn     newReaderFn

	metrics commitLogSourceDataAndIndexMetrics
}

type encoder struct {
	lastWriteAt time.Time
	enc         encoding.Encoder
}

func newCommitLogSource(opts Options, inspection fs.Inspection) bootstrap.Source {
	scope := opts.
		ResultOptions().
		InstrumentOptions().
		MetricsScope().
		SubScope("bootstrapper-commitlog")

	return &commitLogSource{
		opts: opts,
		log: opts.
			ResultOptions().
			InstrumentOptions().
			Logger().
			WithFields(xlog.NewField("bootstrapper", "commitlog")),

		inspection: inspection,

		newIteratorFn:   commitlog.NewIterator,
		snapshotFilesFn: fs.SnapshotFiles,
		newReaderFn:     fs.NewReader,

		metrics: newCommitLogSourceDataAndIndexMetrics(scope),
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
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
}

// ReadData will read a combination of the available snapshot files and commit log files to
// restore as much unflushed data from disk as possible. The logic for performing this
// correctly is as follows:
//
//    1.  For every shard/blockStart combination, find the most recently written and complete
//        (has a checkpoint file) snapshot.
//    2.  For every shard/blockStart combination, determine the most recent complete SnapshotTime.
//        This value corresponds to the (local) moment in time right before the snapshotting process
//        began.
//    3.  Find the minimum SnapshotTime for all of the shards and block starts (call it t0), and
//        replay (M3TSZ encode) all commit log entries whose system timestamps overlap the range
//        [minimumSnapshotTimeAcrossShards, blockStart.Add(blockSize).Add(bufferPast)]. This logic
//        has one exception which is in the case where there is no minimimum snapshot time across
//        shards (the code treats this case as minimum snapshot time across shards == blockStart).
//        In that case, we replay all commit log entries whose system timestamps overlap the range
//        [blockStart.Add(-bufferFuture), blockStart.Add(blockSize).Add(bufferPast)].
//    4.  For each shard/blockStart combination, merge all of the M3TSZ encoders that we created from
//        reading the commit log along with the data available in the corresponding snapshot file.
//
// Example #1:
//
//    BlockSize: 2hr
//    BufferPast: 10m
//    BufferFuture: 5m
//    CommitLogBlockSize: 10m
//    BlockToBootstrap: 12PM->2PM
//    SnapshotTime: 12:30PM
//
//    W1 comes in at 11:57AM
//    W2 comes in at 12:29PM
//    W3 comes in at 12:31PM
//    W4 comes in at 2:04PM
//
//    1) W1 captured by snapshot (hence why we don't need to worry about buffer future
//       with regards to commit logs when a snapshot file is present.)
//    2) W2 captured by snapshot file
//    3) W3 not captured by snapshot file (present in commit log with start time 12:30PM)
//    4) W4 not captured by snapshot file (present in commit log with start time 2:00PM)
//
//    Need to read all commit logs that contain writes with system times spanning from
//    12:30PM -> 2:10PM which will bootstrap all of the data points above. I.E:
//    [minimumMostRecentSnapshotTimeAcrossShards, blockStart.Add(blockSize).Add(bufferPast)]
//
// Example #2:
//
//    BlockSize: 2hr
//    BufferPast: 10m
//    BufferFuture: 5m
//    CommitLogBlockSize: 10m
//    BlockToBootstrap: 12PM->2PM
//    SnapshotTime: 12:00PM (snapshot does not exist)
//
//    W1 comes in at 11:57AM
//    W2 comes in at 12:29PM
//    W3 comes in at 12:31PM
//    W4 comes in at 2:04PM
//
//    1) W1 only present in commit log with start time 11:50PM
//    2) W2 only present in commit log with start time 12:20PM
//    3) W3 only present in commit log with start time 12:30PM
//    4) W4 only present in commit log with start time 2:00PM
//
//    Need to read all commit logs that contain writes with system times spanning from
//    11:55AM -> 2:10PM which will bootstrap all of the data points above. I.E:
//    [blockStart.Add(-bufferFuture), blockStart.Add(blockSize).Add(bufferPast)]
func (s *commitLogSource) ReadData(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.DataBootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return result.NewDataBootstrapResult(), nil
	}

	var (
		// Emit bootstrapping gauge for duration of ReadData
		doneReadingData        = s.metrics.data.emitBootstrapping()
		encounteredCorruptData = false
		fsOpts                 = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix         = fsOpts.FilePathPrefix()
	)
	defer doneReadingData()

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
	readCommitLogPred, mostRecentCompleteSnapshotByBlockShard, err := s.newReadCommitLogPredBasedOnAvailableSnapshotFiles(
		ns, shardsTimeRanges, snapshotFilesByShard)
	if err != nil {
		return nil, err
	}

	// Setup the commit log iterator.
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

	iter, corruptFiles, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}

	if len(corruptFiles) > 0 {
		s.logAndEmitCorruptFiles(corruptFiles, true)
		encounteredCorruptData = true
	}

	defer iter.Close()

	// Setup the M3TSZ encoding pipeline
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

	// Read / M3TSZ encode all the datapoints in the commit log that we need to read.
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
		// Log the error and mark that we encountered corrupt data, but don't
		// return the error because we want to give the peers bootstrapper the
		// opportunity to repair the data instead of failing the bootstrap
		// altogether.
		s.log.Errorf(
			"error in commitlog iterator: %v", iterErr)
		s.metrics.data.corruptCommitlogFile.Inc(1)
		encounteredCorruptData = true
	}

	for _, encoderChan := range encoderChans {
		close(encoderChan)
	}

	// Block until all required data from the commit log has been read and
	// encoded by the worker goroutines
	wg.Wait()
	s.logEncodingOutcome(workerErrs, iter)

	// Merge all the different encoders from the commit log that we created with
	// the data that is available in the snapshot files.
	s.log.Infof("starting merge...")
	mergeStart := time.Now()
	bootstrapResult, err := s.mergeAllShardsCommitLogEncodersAndSnapshots(
		ns,
		shardsTimeRanges,
		snapshotFilesByShard,
		mostRecentCompleteSnapshotByBlockShard,
		int(numShards),
		blockSize,
		shardDataByShard,
	)
	if err != nil {
		return nil, err
	}
	s.log.Infof("done merging..., took: %s", time.Since(mergeStart).String())

	shouldReturnUnfulfilled, err := s.shouldReturnUnfulfilled(
		encounteredCorruptData, ns, shardsTimeRanges, runOpts)
	if err != nil {
		return nil, err
	}

	if shouldReturnUnfulfilled {
		bootstrapResult.SetUnfulfilled(shardsTimeRanges)
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
			series: NewMap(MapOptions{}),
			ranges: shardsTimeRanges[shard],
		}
	}

	return shardDataByShard
}

// mostRecentCompleteSnapshotByBlockShard returns a
// map[xtime.UnixNano]map[uint32]fs.FileSetFile with the contract that
// for each shard/block combination in shardsTimeRanges, an entry will
// exist in the map such that FileSetFile.CachedSnapshotTime is the
// actual cached snapshot time, or the blockStart.
func (s *commitLogSource) mostRecentCompleteSnapshotByBlockShard(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	snapshotFilesByShard map[uint32]fs.FileSetFilesSlice,
	fsOpts fs.Options,
) map[xtime.UnixNano]map[uint32]fs.FileSetFile {
	var (
		minBlock, maxBlock              = shardsTimeRanges.MinMax()
		mostRecentSnapshotsByBlockShard = map[xtime.UnixNano]map[uint32]fs.FileSetFile{}
	)

	for currBlockStart := minBlock.Truncate(blockSize); currBlockStart.Before(maxBlock); currBlockStart = currBlockStart.Add(blockSize) {
		for shard := range shardsTimeRanges {
			// Anonymous func for easier clean up using defer.
			func() {
				var (
					currBlockUnixNanos = xtime.ToUnixNano(currBlockStart)
					mostRecentSnapshot fs.FileSetFile
				)

				defer func() {
					existing := mostRecentSnapshotsByBlockShard[currBlockUnixNanos]
					if existing == nil {
						existing = map[uint32]fs.FileSetFile{}
					}

					if mostRecentSnapshot.IsZero() {
						// If we were unable to determine the most recent snapshot time for a given
						// shard/blockStart combination, then just fall back to using the blockStart
						// time as that will force us to read the entire commit log for that duration.
						mostRecentSnapshot.CachedSnapshotTime = currBlockStart
					}
					existing[shard] = mostRecentSnapshot
					mostRecentSnapshotsByBlockShard[currBlockUnixNanos] = existing
				}()

				snapshotFiles, ok := snapshotFilesByShard[shard]
				if !ok {
					// If there are no snapshot files for this shard, then rely on
					// the defer to fallback to using the block start time.
					return
				}

				mostRecentSnapshotVolume, ok := snapshotFiles.LatestVolumeForBlock(currBlockStart)
				if !ok {
					// If there are no complete snapshot files for this block, then rely on
					// the defer to fallback to using the block start time.
					return
				}

				// Make sure we're able to read the snapshot time. This will also set the
				// CachedSnapshotTime field so that we can rely upon it from here on out.
				_, _, err := mostRecentSnapshotVolume.SnapshotTimeAndID()
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

					// If we couldn't determine the snapshot time for the snapshot file, then rely
					// on the defer to fallback to using the block start time.
					return
				}

				mostRecentSnapshot = mostRecentSnapshotVolume
			}()
		}
	}

	return mostRecentSnapshotsByBlockShard
}

func (s *commitLogSource) minimumMostRecentSnapshotTimeByBlock(
	shardsTimeRanges result.ShardTimeRanges,
	blockSize time.Duration,
	mostRecentSnapshotByBlockShard map[xtime.UnixNano]map[uint32]fs.FileSetFile,
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

			if mostRecentSnapshotForShard.CachedSnapshotTime.Before(minMostRecentSnapshot) || minMostRecentSnapshot.IsZero() {
				minMostRecentSnapshot = mostRecentSnapshotForShard.CachedSnapshotTime
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

func (s *commitLogSource) bootstrapShardSnapshots(
	nsID ident.ID,
	shard uint32,
	metadataOnly bool,
	shardTimeRanges xtime.Ranges,
	blockSize time.Duration,
	snapshotFiles fs.FileSetFilesSlice,
	mostRecentCompleteSnapshotByBlockShard map[xtime.UnixNano]map[uint32]fs.FileSetFile,
) (result.ShardResult, error) {
	var (
		shardResult    result.ShardResult
		allSeriesSoFar *result.Map
		rangeIter      = shardTimeRanges.Iter()
		err            error
	)

	for rangeIter.Next() {

		var (
			currRange             = rangeIter.Value()
			currRangeDuration     = currRange.End.Sub(currRange.Start)
			isMultipleOfBlockSize = currRangeDuration%blockSize == 0
		)

		if !isMultipleOfBlockSize {
			return nil, fmt.Errorf(
				"received bootstrap range that is not multiple of blockSize, blockSize: %d, start: %s, end: %s",
				blockSize, currRange.End.String(), currRange.Start.String(),
			)
		}

		// Reset this after we bootstrap each block to make sure its up to date.
		if shardResult != nil {
			allSeriesSoFar = shardResult.AllSeries()
		}

		for blockStart := currRange.Start.Truncate(blockSize); blockStart.Before(currRange.End); blockStart = blockStart.Add(blockSize) {
			snapshotsForBlock := mostRecentCompleteSnapshotByBlockShard[xtime.ToUnixNano(blockStart)]
			mostRecentCompleteSnapshotForShardBlock := snapshotsForBlock[shard]

			if mostRecentCompleteSnapshotForShardBlock.CachedSnapshotTime.Equal(blockStart) ||
				// Should never happen
				mostRecentCompleteSnapshotForShardBlock.IsZero() {
				// There is no snapshot file for this time, and even if there was, there would
				// be no point in reading it. In this specific case its not an error scenario
				// because the fact that snapshotTime == blockStart means we already accounted
				// for the fact that this snapshot did not exist when we were deciding which
				// commit logs to read.
				s.log.Debugf(
					"no snapshots for shard: %d and blockStart: %s",
					shard, blockStart.String())
				continue
			}

			shardResult, err = s.bootstrapShardBlockSnapshot(
				nsID, shard, blockStart, metadataOnly, shardResult, allSeriesSoFar, blockSize,
				snapshotFiles, mostRecentCompleteSnapshotForShardBlock)
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
	metadataOnly bool,
	shardResult result.ShardResult,
	allSeriesSoFar *result.Map,
	blockSize time.Duration,
	snapshotFiles fs.FileSetFilesSlice,
	mostRecentCompleteSnapshot fs.FileSetFile,
) (result.ShardResult, error) {
	var (
		bOpts      = s.opts.ResultOptions()
		blOpts     = bOpts.DatabaseBlockOptions()
		blocksPool = blOpts.DatabaseBlockPool()
		bytesPool  = blOpts.BytesPool()
		fsOpts     = s.opts.CommitLogOptions().FilesystemOptions()
		idPool     = s.opts.CommitLogOptions().IdentifierPool()
	)

	// Bootstrap the snapshot file
	reader, err := s.newReaderFn(bytesPool, fsOpts)
	if err != nil {
		return shardResult, err
	}

	err = reader.Open(fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   nsID,
			BlockStart:  blockStart,
			Shard:       shard,
			VolumeIndex: mostRecentCompleteSnapshot.ID.VolumeIndex,
		},
		FileSetType: persist.FileSetSnapshotType,
	})
	if err != nil {
		return shardResult, err
	}

	s.log.Debugf(
		"reading snapshot for shard: %d and blockStart: %s and volume: %d",
		shard, blockStart.String(), mostRecentCompleteSnapshot.ID.VolumeIndex)
	for {
		var (
			id               ident.ID
			tagsIter         ident.TagIterator
			data             checked.Bytes
			expectedChecksum uint32
		)
		if metadataOnly {
			id, tagsIter, _, _, err = reader.ReadMetadata()
		} else {
			id, tagsIter, data, expectedChecksum, err = reader.Read()
		}
		if err != nil && err != io.EOF {
			return shardResult, err
		}

		if err == io.EOF {
			break
		}

		dbBlock := blocksPool.Get()
		dbBlock.Reset(blockStart, blockSize, ts.NewSegment(data, nil, ts.FinalizeHead))

		if !metadataOnly {
			// Resetting the block will trigger a checksum calculation, so use that instead
			// of calculating it twice.
			checksum, err := dbBlock.Checksum()
			if err != nil {
				return shardResult, err
			}

			if checksum != expectedChecksum {
				return shardResult, fmt.Errorf("checksum for series: %s was %d but expected %d", id, checksum, expectedChecksum)
			}
		}

		var (
			tags             ident.Tags
			shouldDecodeTags = true
		)
		if allSeriesSoFar != nil {
			if existing, ok := allSeriesSoFar.Get(id); ok {
				// If we've already bootstrapped this series for a different block, we don't need
				// another copy of the IDs and tags.
				id.Finalize()
				id = existing.ID
				tags = existing.Tags
				shouldDecodeTags = false
			}
		}

		if shouldDecodeTags {
			// Only spend cycles decoding the tags if we've never seen them before.
			if tagsIter.Remaining() > 0 {
				tags, err = convert.TagsFromTagsIter(id, tagsIter, idPool)
				if err != nil {
					return shardResult, fmt.Errorf("unable to decode tags: %v", err)
				}
			}
		}

		// Always close even if we didn't use it.
		tagsIter.Close()

		// Mark the ID and Tags as no finalize to enable no-copy optimization later
		// in the bootstrap process (when they're being loaded into the shard). Also,
		// technically we'll be calling NoFinalize() repeatedly on the same IDs for
		// different blocks since we're reusing them, but thats ok as it an idempotent
		// operation and there is no concurrency here.
		id.NoFinalize()
		tags.NoFinalize()

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
) (
	commitlog.FileFilterPredicate,
	map[xtime.UnixNano]map[uint32]fs.FileSetFile,
	error,
) {
	blockSize := ns.Options().RetentionOptions().BlockSize()

	// At this point we've bootstrapped all the snapshot files that we can, and we need to
	// decide which commit logs to read. In order to do that, we'll need to figure out the
	// minimum most recent snapshot time for each block, then we can use that information to
	// decide how much of the commit log we need to read for each block that we're bootstrapping.
	// To start, for each block that we're bootstrapping, we need to figure out the most recent
	// snapshot that was taken for each shard. I.E we want to create a datastructure that looks
	// like this:
	// 		map[blockStart]map[shard]mostRecentSnapshotTime
	mostRecentCompleteSnapshotByBlockShard := s.mostRecentCompleteSnapshotByBlockShard(
		shardsTimeRanges, blockSize, snapshotFilesByShard, s.opts.CommitLogOptions().FilesystemOptions())
	for block, mostRecentByShard := range mostRecentCompleteSnapshotByBlockShard {
		for shard, mostRecent := range mostRecentByShard {

			if mostRecent.CachedSnapshotTime.IsZero() {
				// Should never happen.
				return nil, nil, instrument.InvariantErrorf(
					"shard: %d and block: %s had zero value for most recent snapshot time",
					shard, block.ToTime().String())
			}

			s.log.Debugf(
				"most recent snapshot for block: %s and shard: %d is %s",
				block.ToTime().String(), shard, mostRecent.CachedSnapshotTime.String())
		}
	}

	// Once we have the desired data structure, we next need to figure out the minimum most recent
	// snapshot for that block across all shards. This will help us determine how much of the commit
	// log we need to read. The new data structure we're trying to generate looks like:
	// 		map[blockStart]minimumMostRecentSnapshotTime (across all shards)
	// This structure is important because it tells us how much of the commit log we need to read for
	// each block that we're trying to bootstrap (because the commit log is shared across all shards.)
	minimumMostRecentSnapshotTimeByBlock := s.minimumMostRecentSnapshotTimeByBlock(
		shardsTimeRanges, blockSize, mostRecentCompleteSnapshotByBlockShard)
	for block, minSnapshotTime := range minimumMostRecentSnapshotTimeByBlock {
		s.log.Debugf(
			"min snapshot time for block: %s is: %s",
			block.ToTime().String(), minSnapshotTime.String())
	}

	// Now that we have the minimum most recent snapshot time for each block, we can use that data to
	// decide how much of the commit log we need to read for each block that we're bootstrapping. We'll
	// construct a new predicate based on the data structure we constructed earlier where the new
	// predicate will check if there is any overlap between a commit log file and a temporary range
	// we construct that begins with the minimum snapshot time and ends with the end of that block + bufferPast.
	return s.newReadCommitLogPred(ns, minimumMostRecentSnapshotTimeByBlock), mostRecentCompleteSnapshotByBlockShard, nil
}

func (s *commitLogSource) newReadCommitLogPred(
	ns namespace.Metadata,
	minimumMostRecentSnapshotTimeByBlock map[xtime.UnixNano]time.Time,
) commitlog.FileFilterPredicate {
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
	// be a small win in terms of memory utilization.
	return func(f persist.CommitlogFile) bool {
		_, ok := commitlogFilesPresentBeforeStart[f.FilePath]
		if !ok {
			// If the file wasn't on disk before the node started then it only contains
			// writes that are already in memory (and in-fact the file may be actively
			// being written to.)
			return false
		}

		for _, rangeToCheck := range rangesToCheck {
			commitLogEntryRange := xtime.Range{
				Start: f.Start,
				End:   f.Start.Add(f.Duration),
			}

			if commitLogEntryRange.Overlaps(rangeToCheck) {
				s.log.
					Infof(
						"opting to read commit log: %s with start: %s and duration: %s",
						f.FilePath, f.Start.String(), f.Duration.String())
				return true
			}
		}

		s.log.
			Infof(
				"opting to skip commit log: %s with start: %s and duration: %s",
				f.FilePath, f.Start.String(), f.Duration.String())
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
	for arg := range ec {
		var (
			series     = arg.series
			dp         = arg.dp
			unit       = arg.unit
			annotation = arg.annotation
			blockStart = arg.blockStart
		)

		var (
			unmergedShard      = unmerged[series.Shard].series
			unmergedSeries, ok = unmergedShard.Get(series.ID)
		)
		if !ok {
			unmergedSeries = metadataAndEncodersByTime{
				id:       series.ID,
				tags:     series.Tags,
				encoders: make(map[xtime.UnixNano][]encoder)}
			// Have to use unsafe because we don't want to copy the IDs we put
			// into this map because its lifecycle is much shorter than that of
			// the IDs we're putting into it so copying would waste too much
			// memory unnecessarily, and we don't want to finalize the IDs for the
			// same reason.
			unmergedShard.SetUnsafe(
				series.ID, unmergedSeries,
				SetUnsafeOptions{NoCopyKey: true, NoFinalizeKey: true})
		}

		var (
			err            error
			blockStartNano = xtime.ToUnixNano(blockStart)
			unmergedBlock  = unmergedSeries.encoders[blockStartNano]
			wroteExisting  = false
		)
		for i := range unmergedBlock {
			// TODO(r): Write unit test to ensure that different values that arrive
			// later in the commit log will upsert the previous value when bootstrapping
			// Tracking with issue: https://github.com/m3db/m3/issues/898
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
	series ts.Series,
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

func (s *commitLogSource) mergeAllShardsCommitLogEncodersAndSnapshots(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	snapshotFiles map[uint32]fs.FileSetFilesSlice,
	mostRecentCompleteSnapshotByBlockShard map[xtime.UnixNano]map[uint32]fs.FileSetFile,
	numShards int,
	blockSize time.Duration,
	unmerged []shardData,
) (result.DataBootstrapResult, error) {
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
		if unmergedShard.series == nil {
			// Not bootstrapping this shard
			continue
		}

		snapshotData, err := s.bootstrapShardSnapshots(
			ns.ID(),
			uint32(shard),
			false,
			shardsTimeRanges[uint32(shard)],
			blockSize,
			snapshotFiles[uint32(shard)],
			mostRecentCompleteSnapshotByBlockShard,
		)
		if err != nil {
			bootstrapResultLock.Lock()
			// Mark the shard time ranges as unfulfilled so a subsequent bootstrapper
			// has the chance to fulfill it.
			bootstrapResult.Add(
				uint32(shard),
				result.NewShardResult(0, s.opts.ResultOptions()),
				shardsTimeRanges[uint32(shard)],
			)
			bootstrapResultLock.Unlock()
			return nil, err
		}

		// Merge snapshot and commit log data
		wg.Add(1)
		shard, unmergedShard := shard, unmergedShard
		mergeShardFunc := func() {
			var shardResult result.ShardResult
			shardResult, shardEmptyErrs[shard], shardErrs[shard] = s.mergeShardCommitLogEncodersAndSnapshots(
				shard, snapshotData, unmergedShard, blockSize)

			if shardResult != nil && shardResult.NumSeries() > 0 {
				// Prevent race conditions while updating bootstrapResult from multiple go-routines
				bootstrapResultLock.Lock()
				if shardEmptyErrs[shard] != 0 || shardErrs[shard] != 0 {
					// If there were any errors, keep the data but mark the shard time ranges as
					// unfulfilled so a subsequent bootstrapper has the chance to fulfill it.
					bootstrapResult.Add(uint32(shard), shardResult, shardsTimeRanges[uint32(shard)])
				} else {
					bootstrapResult.Add(uint32(shard), shardResult, xtime.Ranges{})
				}
				bootstrapResultLock.Unlock()
			}
			wg.Done()
		}
		workerPool.Go(mergeShardFunc)
	}

	// Wait for all merge goroutines to complete
	wg.Wait()
	s.logMergeShardsOutcome(shardErrs, shardEmptyErrs)
	return bootstrapResult, nil
}

func (s *commitLogSource) mergeShardCommitLogEncodersAndSnapshots(
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

	numSeries := 0
	if unmergedShard.series != nil {
		numSeries = unmergedShard.series.Len()
	}

	var (
		shardResult       = result.NewShardResult(numSeries, s.opts.ResultOptions())
		numShardEmptyErrs int
		numErrs           int
	)

	allSnapshotSeries := snapshotData.AllSeries()

	if unmergedShard.series != nil {
		for _, unmergedBlocks := range unmergedShard.series.Iter() {
			val := unmergedBlocks.Value()
			snapshotSeriesData, _ := allSnapshotSeries.Get(val.id)
			seriesBlocks, numSeriesEmptyErrs, numSeriesErrs := s.mergeSeries(
				snapshotSeriesData,
				val,
				blocksPool,
				multiReaderIteratorPool,
				segmentReaderPool,
				encoderPool,
				blockSize,
				blOpts,
			)

			if seriesBlocks != nil && seriesBlocks.Len() > 0 {
				shardResult.AddSeries(val.id, val.tags, seriesBlocks)
			}

			numShardEmptyErrs += numSeriesEmptyErrs
			numErrs += numSeriesErrs
		}
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
		var (
			start            = startNano.ToTime()
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
			numErrs++
			continue
		}

		iter := multiReaderIteratorPool.Get()
		iter.Reset(readers, time.Time{}, 0)

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
		readers.close()
		if hasSnapshotBlock {
			// Block is already closed, but we need to remove from the Blocks
			// to prevent a double free when we call Blocks.Close() later.
			snapshotData.Blocks.RemoveBlockAt(start)
		}

		if err != nil {
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
				// Should never happen because we would have called
				// Blocks.RemoveBlockAt() above.
				iOpts := s.opts.CommitLogOptions().InstrumentOptions()
				instrument.EmitAndLogInvariantViolation(iOpts, func(l xlog.Logger) {
					l.Errorf(
						"tried to merge block that should have been removed, blockStart: %d", startNano)
				})
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

func (s *commitLogSource) AvailableIndex(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	return s.availability(ns, shardsTimeRanges, runOpts)
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
		// Emit bootstrapping gauge for duration of ReadIndex
		doneReadingIndex       = s.metrics.index.emitBootstrapping()
		encounteredCorruptData = false
		fsOpts                 = s.opts.CommitLogOptions().FilesystemOptions()
		filePathPrefix         = fsOpts.FilePathPrefix()
	)
	defer doneReadingIndex()

	// Determine which snapshot files are available.
	snapshotFilesByShard, err := s.snapshotFilesByShard(
		ns.ID(), filePathPrefix, shardsTimeRanges)
	if err != nil {
		return nil, err
	}

	var (
		highestShard = s.findHighestShard(shardsTimeRanges)
		// +1 so we can use the shard number as an index throughout without constantly
		// remembering to subtract 1 to convert to zero-based indexing.
		numShards = highestShard + 1
		// Convert the map to a slice for faster lookups
		bootstrapRangesByShard = make([]xtime.Ranges, numShards)
	)

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

	// Determine which commit log files we need to read based on which snapshot
	// snapshot files are available.
	readCommitLogPredicate, mostRecentCompleteSnapshotByBlockShard, err := s.newReadCommitLogPredBasedOnAvailableSnapshotFiles(
		ns, shardsTimeRanges, snapshotFilesByShard)
	if err != nil {
		return nil, err
	}

	var (
		readSeriesPredicate = newReadSeriesPredicate(ns)
		iterOpts            = commitlog.IteratorOpts{
			CommitLogOptions:      s.opts.CommitLogOptions(),
			FileFilterPredicate:   readCommitLogPredicate,
			SeriesFilterPredicate: readSeriesPredicate,
		}
	)

	// Start by reading any available snapshot files.
	for shard, tr := range shardsTimeRanges {
		shardResult, err := s.bootstrapShardSnapshots(
			ns.ID(), shard, true, tr, blockSize, snapshotFilesByShard[shard],
			mostRecentCompleteSnapshotByBlockShard)
		if err != nil {
			return nil, err
		}

		// Bootstrap any series we got from the snapshot files into the index.
		for _, val := range shardResult.AllSeries().Iter() {
			id := val.Key()
			val := val.Value()
			for block := range val.Blocks.AllBlocks() {
				s.maybeAddToIndex(
					id, val.Tags, shard, highestShard, block.ToTime(), bootstrapRangesByShard,
					indexResults, indexOptions, indexBlockSize, resultOptions)
			}
		}
	}

	// Next, read all of the data from the commit log files that wasn't covered
	// by the snapshot files.
	iter, corruptFiles, err := s.newIteratorFn(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}
	if len(corruptFiles) > 0 {
		s.logAndEmitCorruptFiles(corruptFiles, false)
		encounteredCorruptData = true
	}

	defer iter.Close()

	for iter.Next() {
		series, dp, _, _ := iter.Current()

		s.maybeAddToIndex(
			series.ID, series.Tags, series.Shard, highestShard, dp.Timestamp, bootstrapRangesByShard,
			indexResults, indexOptions, indexBlockSize, resultOptions)
	}

	if iterErr := iter.Err(); iterErr != nil {
		// Log the error and mark that we encountered corrupt data, but don't
		// return the error because we want to give the peers bootstrapper the
		// opportunity to repair the data instead of failing the bootstrap
		// altogether.
		s.log.Errorf(
			"error in commitlog iterator: %v", iterErr)
		encounteredCorruptData = true
		s.metrics.index.corruptCommitlogFile.Inc(1)
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

	shouldReturnUnfulfilled, err := s.shouldReturnUnfulfilled(
		encounteredCorruptData, ns, shardsTimeRanges, opts)
	if err != nil {
		return nil, err
	}

	if shouldReturnUnfulfilled {
		indexResult.SetUnfulfilled(shardsTimeRanges)
	}
	return indexResult, nil
}

// If we encountered any corrupt data and there is a possibility of the
// peers bootstrapper being able to correct it, we want to mark the entire range
// as unfulfilled so the peers bootstrapper can attempt a repair, but keep
// the data we read from the commit log as well in case the peers
// bootstrapper is unable to satisfy the bootstrap because all peers are
// down or if the commitlog contained data that the peers do not have.
func (s commitLogSource) shouldReturnUnfulfilled(
	encounteredCorruptData bool,
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts bootstrap.RunOptions,
) (bool, error) {
	if !s.opts.ReturnUnfulfilledForCorruptCommitlogFiles() {
		return false, nil
	}

	if !encounteredCorruptData {
		return false, nil
	}

	areShardsReplicated := s.areShardsReplicated(
		ns, shardsTimeRanges, opts)

	return areShardsReplicated, nil
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

	// We can use the NoClone variant here because the IDs/Tags read from the commit log files
	// by the ReadIndex() method won't be finalized because this code path doesn't finalize them.
	d, err := convert.FromMetricNoClone(id, tags)
	if err != nil {
		return err
	}

	_, err = segment.Insert(d)
	return err
}

func (s *commitLogSource) logAndEmitCorruptFiles(
	corruptFiles []commitlog.ErrorWithPath, isData bool) {
	for _, f := range corruptFiles {
		s.log.
			Errorf(
				"opting to skip commit log due to corruption: %s", f.Error())
		if isData {
			s.metrics.data.corruptCommitlogFile.Inc(1)
		} else {
			s.metrics.index.corruptCommitlogFile.Inc(1)
		}
	}
}

// The commitlog bootstrapper determines availability primarily by checking if the
// origin host has ever reached the "Available" state for the shard that is being
// bootstrapped. If not, then it can't provide data for that shard because it doesn't
// have all of it by definition.
func (s *commitLogSource) availability(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) (result.ShardTimeRanges, error) {
	var (
		topoState                = runOpts.InitialTopologyState()
		availableShardTimeRanges = result.ShardTimeRanges{}
	)

	for shardIDUint := range shardsTimeRanges {
		shardID := topology.ShardID(shardIDUint)
		hostShardStates, ok := topoState.ShardStates[shardID]
		if !ok {
			// This shard was not part of the topology when the bootstrapping
			// process began.
			continue
		}

		originHostShardState, ok := hostShardStates[topology.HostID(topoState.Origin.ID())]
		if !ok {
			errMsg := fmt.Sprintf("initial topology state does not contain shard state for origin node and shard: %d", shardIDUint)
			iOpts := s.opts.CommitLogOptions().InstrumentOptions()
			instrument.EmitAndLogInvariantViolation(iOpts, func(l xlog.Logger) {
				l.Error(errMsg)
			})
			return nil, errors.New(errMsg)
		}

		originShardState := originHostShardState.ShardState
		switch originShardState {
		// In the Initializing state we have to assume that the commit log
		// is missing data and can't satisfy the bootstrap request.
		case shard.Initializing:
		// In the Leaving and Available case, we assume that the commit log contains
		// all the data required to satisfy the bootstrap request because the node
		// had (at some point) been completely bootstrapped for the requested shard.
		// This doesn't mean that the node can't be missing any data or wasn't down
		// for some period of time and missing writes in a multi-node deployment, it
		// only means that technically the node has successfully taken ownership of
		// the data for this shard and made it to a "bootstrapped" state which is
		// all that is required to maintain our cluster-level consistency guarantees.
		case shard.Leaving:
			fallthrough
		case shard.Available:
			// Assume that we can bootstrap any requested time range, which is valid as
			// long as the FS bootstrapper precedes the commit log bootstrapper.
			// TODO(rartoul): Once we make changes to the bootstrapping interfaces
			// to distinguish between "unfulfilled" data and "corrupt" data, then
			// modify this to only say the commit log bootstrapper can fullfil
			// "unfulfilled" data, but not corrupt data.
			availableShardTimeRanges[shardIDUint] = shardsTimeRanges[shardIDUint]
		case shard.Unknown:
			fallthrough
		default:
			return result.ShardTimeRanges{}, fmt.Errorf("unknown shard state: %v", originShardState)
		}
	}

	return availableShardTimeRanges, nil
}

func (s *commitLogSource) areShardsReplicated(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	runOpts bootstrap.RunOptions,
) bool {
	var (
		initialTopologyState = runOpts.InitialTopologyState()
		majorityReplicas     = initialTopologyState.MajorityReplicas
	)

	// In any situation where we could actually stream data from our peers
	// the replication factor would be 2 or larger which means that the
	// value of majorityReplicas would be 2 or larger also. This heuristic can
	// only be used to infer whether the replication factor is 1 or larger, but
	// cannot be used to determine what the actual replication factor is in all
	// situations because it can be ambiguous. For example, both R.F 2 and 3 will
	// have majority replica values of 2.
	return majorityReplicas > 1
}

func newReadSeriesPredicate(ns namespace.Metadata) commitlog.SeriesFilterPredicate {
	nsID := ns.ID()
	return func(id ident.ID, namespace ident.ID) bool {
		return nsID.Equal(namespace)
	}
}

type shardData struct {
	series *Map
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
	series     ts.Series
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

type commitLogSourceDataAndIndexMetrics struct {
	data  commitLogSourceMetrics
	index commitLogSourceMetrics
}

func newCommitLogSourceDataAndIndexMetrics(scope tally.Scope) commitLogSourceDataAndIndexMetrics {
	return commitLogSourceDataAndIndexMetrics{
		data: newCommitLogSourceMetrics(scope.Tagged(map[string]string{
			"source_type": "data",
		})),
		index: newCommitLogSourceMetrics(scope.Tagged(map[string]string{
			"source_type": "index",
		})),
	}
}

type commitLogSourceMetrics struct {
	corruptCommitlogFile tally.Counter
	bootstrapping        tally.Gauge
}

type gaugeLoopCloserFn func()

func (m commitLogSourceMetrics) emitBootstrapping() gaugeLoopCloserFn {
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-doneCh:
				m.bootstrapping.Update(0)
				return
			default:
				m.bootstrapping.Update(1)
				time.Sleep(time.Second)
			}
		}
	}()

	return func() { close(doneCh) }
}

func newCommitLogSourceMetrics(scope tally.Scope) commitLogSourceMetrics {
	return commitLogSourceMetrics{
		corruptCommitlogFile: scope.SubScope("commitlog").Counter("corrupt"),
		bootstrapping:        scope.SubScope("status").Gauge("bootstrapping"),
	}
}
