// Copyright (c) 2017 Uber Technologies, Inc.
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

package main

import (
	"flag"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3db/src/dbnode/retention"
	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper"
	commitlogsrc "github.com/m3db/m3db/src/dbnode/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/dbnode/tools"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"
)

var flagParser = flag.NewFlagSet("Verify Commitlogs", flag.ExitOnError)

var (
	pathPrefixArg         = flagParser.String("path-prefix", "/var/lib/m3db", "Path prefix - must contain a folder called 'commitlogs'")
	namespaceArg          = flagParser.String("namespace", "metrics", "Namespace")
	blockSizeArg          = flagParser.Duration("block-size", 10*time.Minute, "Block size")
	flushSizeArg          = flagParser.Int("flush-size", 524288, "Flush size of commit log")
	bootstrapRetentionArg = flagParser.Duration("retention", 48*time.Hour, "Retention")
	shardsCountArg        = flagParser.Int("shards-count", 8192, "Shards count - set number too bootstrap all shards in range")
	shardsArg             = flagParser.String("shards", "", "Shards - set comma separated list of shards")
	debugListenAddressArg = flagParser.String("debug-listen-address", "", "Debug listen address - if set will expose pprof, i.e. ':8080'")
	startUnixTimestampArg = flagParser.Int64("start-unix-timestamp", 0, "Start unix timestamp (Seconds) - If set will bootstrap all data after this timestamp up to end-unix-timestamp, defaults to reading from the beginning of the first commitlog")
	// 1<<63-62135596801 is the largest possible time.Time that can be represented
	// without causing overflow when passed to functions in the time package
	endUnixTimestampArg    = flagParser.Int64("end-unix-timestamp", 1<<63-62135596801, "End unix timestamp (Seconds) - If set will bootstrap all data from start-unix-timestamp up to this timestamp, defaults to reading up to the end of the last commitlog")
	readConcurrency        = flagParser.Int("read-concurrency", 4, "Commitlog read concurrency")
	encodingConcurrency    = flagParser.Int("encoding-concurrency", 4, "Encoding concurrency")
	mergeShardsConcurrency = flagParser.Int("merge-shards-concurrency", 4, "Merge shards concurrency")
)

func main() {
	flagParser.Parse(os.Args[1:])

	var (
		pathPrefix         = *pathPrefixArg
		namespaceStr       = *namespaceArg
		blockSize          = *blockSizeArg
		flushSize          = *flushSizeArg
		bootstrapRetention = *bootstrapRetentionArg
		shardsCount        = *shardsCountArg
		shards             = *shardsArg
		debugListenAddress = *debugListenAddressArg
		startUnixTimestamp = *startUnixTimestampArg
		endUnixTimestamp   = *endUnixTimestampArg
	)

	log := xlog.NewLogger(os.Stderr)

	if debugListenAddress != "" {
		go func() {
			log.Infof("starting debug listen server at '%s'\n", debugListenAddress)
			err := http.ListenAndServe(debugListenAddress, http.DefaultServeMux)
			if err != nil {
				log.Fatalf("could not start debug listen server at '%s': %v", debugListenAddress, err)
			}
		}()
	}

	shardTimeRanges := result.ShardTimeRanges{}

	start := time.Unix(startUnixTimestamp, 0)
	end := time.Unix(endUnixTimestamp, 0)

	// Ony used for logging
	var shardsAll []uint32
	// Handle comma-delimited shard list 1,3,5, etc
	if strings.TrimSpace(shards) != "" {
		for _, shard := range strings.Split(shards, ",") {
			shard = strings.TrimSpace(shard)
			if shard == "" {
				log.Fatalf("Invalid shard list: '%s'", shards)
			}
			value, err := strconv.Atoi(shard)
			if err != nil {
				log.Fatalf("could not parse shard '%s': %v", shard, err)
			}
			rng := xtime.Range{Start: start, End: end}
			shardTimeRanges[uint32(value)] = xtime.Ranges{}.AddRange(rng)
			shardsAll = append(shardsAll, uint32(value))
		}
		// Or just handled up to N (shard-count) shards
	} else if shardsCount > 0 {
		for i := uint32(0); i < uint32(shardsCount); i++ {
			rng := xtime.Range{Start: start, End: end}
			shardTimeRanges[i] = xtime.Ranges{}.AddRange(rng)
			shardsAll = append(shardsAll, i)
		}
	} else {
		log.Info("Either the shards or shards-count argument need to be valid")
		flag.Usage()
		os.Exit(1)
	}

	log.WithFields(
		xlog.NewField("pathPrefix", pathPrefix),
		xlog.NewField("namespace", namespaceStr),
		xlog.NewField("shards", shardsAll),
	).Infof("configured")

	instrumentOpts := instrument.NewOptions().
		SetLogger(log)

	retentionOpts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(bootstrapRetention).
		SetBufferPast(1 * time.Minute).
		SetBufferFuture(1 * time.Minute)

	blockOpts := block.NewOptions()

	encoderPoolOpts := pool.
		NewObjectPoolOptions().
		SetSize(25165824).
		SetRefillLowWatermark(0.001).
		SetRefillHighWatermark(0.002)
	encoderPool := encoding.NewEncoderPool(encoderPoolOpts)

	iteratorPoolOpts := pool.NewObjectPoolOptions().
		SetSize(2048).
		SetRefillLowWatermark(0.01).
		SetRefillHighWatermark(0.02)
	iteratorPool := encoding.NewReaderIteratorPool(iteratorPoolOpts)
	multiIteratorPool := encoding.NewMultiReaderIteratorPool(iteratorPoolOpts)

	segmentReaderPoolOpts := pool.NewObjectPoolOptions().
		SetSize(16384).
		SetRefillLowWatermark(0.01).
		SetRefillHighWatermark(0.02)
	segmentReaderPool := xio.NewSegmentReaderPool(segmentReaderPoolOpts)

	bytesPool := tools.NewCheckedBytesPool()

	encodingOpts := encoding.NewOptions().
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetBytesPool(bytesPool).
		SetSegmentReaderPool(segmentReaderPool)

	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(time.Time{}, nil, true, encodingOpts)
	})

	iteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, true, encodingOpts)
	})

	multiIteratorPool.Init(func(r io.Reader) encoding.ReaderIterator {
		iter := iteratorPool.Get()
		iter.Reset(r)
		return iter
	})
	bytesPool.Init()

	segmentReaderPool.Init()

	blockPoolOpts := pool.NewObjectPoolOptions().
		SetSize(4194304).
		SetRefillLowWatermark(0.001).
		SetRefillHighWatermark(0.002)
	blockPool := block.NewDatabaseBlockPool(blockPoolOpts)
	blockPool.Init(func() block.DatabaseBlock {
		return block.NewDatabaseBlock(time.Time{}, 0, ts.Segment{}, blockOpts)
	})

	blockOpts = blockOpts.
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetMultiReaderIteratorPool(multiIteratorPool).
		SetDatabaseBlockPool(blockPool).
		SetSegmentReaderPool(segmentReaderPool)

	resultOpts := result.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetDatabaseBlockOptions(blockOpts)

	fsOpts := fs.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetFilePathPrefix(pathPrefix)

	commitLogOpts := commitlog.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetFilesystemOptions(fsOpts).
		SetFlushSize(flushSize).
		SetBlockSize(blockSize).
		SetReadConcurrency(*readConcurrency).
		SetBytesPool(bytesPool)

	opts := commitlogsrc.NewOptions().
		SetResultOptions(resultOpts).
		SetCommitLogOptions(commitLogOpts).
		SetEncodingConcurrency(*encodingConcurrency).
		SetMergeShardsConcurrency(*mergeShardsConcurrency)

	log.Infof("bootstrapping")

	// Don't bootstrap anything else
	next := bootstrapper.NewNoOpAllBootstrapperProvider()
	inspection, err := fs.InspectFilesystem(fsOpts)
	if err != nil {
		log.Fatal(err.Error())
	}
	provider, err := commitlogsrc.NewCommitLogBootstrapperProvider(opts, inspection, next)
	if err != nil {
		log.Fatal(err.Error())
	}

	source := provider.Provide()

	nsID := ident.StringID(namespaceStr)
	runOpts := bootstrap.NewRunOptions().
		// Dont save intermediate results
		SetIncremental(false)
	nsMetadata, err := namespace.NewMetadata(nsID, namespace.NewOptions().SetRetentionOptions(retentionOpts))
	if err != nil {
		log.Fatal(err.Error())
	}
	result, err := source.BootstrapData(nsMetadata, shardTimeRanges, runOpts)
	if err != nil {
		log.Fatalf("failed to bootstrap: %v", err)
	}

	log.WithFields(
		xlog.NewField("shardResults", len(result.ShardResults())),
		xlog.NewField("unfulfilled", len(result.Unfulfilled())),
	).Infof("bootstrapped")

	for shard, result := range result.ShardResults() {
		log.WithFields(
			xlog.NewField("shard", shard),
			xlog.NewField("series", result.AllSeries().Len()),
		).Infof("shard result")
	}
}
