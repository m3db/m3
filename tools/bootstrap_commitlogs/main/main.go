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

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	commitlogsrc "github.com/m3db/m3db/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

var (
	pathPrefixArg         = flag.String("path-prefix", "/var/lib/m3db", "Path prefix")
	namespaceArg          = flag.String("namespace", "metrics", "Namespace")
	blockSizeArg          = flag.String("block-size", "2h", "Block size")
	bootstrapRetentionArg = flag.String("retention", "48h", "Retention")
	shardsCountArg        = flag.Int("shards-count", 8192, "Shards count - set number too bootstrap all shards in range")
	shardsArg             = flag.String("shards", "", "Shards - set comma separated list of shards")
	debugListenAddressArg = flag.String("debug-listen-address", "", "Debug listen address - if set will expose pprof, i.e. ':8080'")
)

func main() {
	flag.Parse()
	if *pathPrefixArg == "" ||
		*namespaceArg == "" ||
		*blockSizeArg == "" ||
		*bootstrapRetentionArg == "" {
		flag.Usage()
		os.Exit(1)
	}

	var (
		pathPrefix         = *pathPrefixArg
		namespace          = *namespaceArg
		blockSize          = *blockSizeArg
		bootstrapRetention = *bootstrapRetentionArg
		shardsCount        = *shardsCountArg
		shards             = *shardsArg
		debugListenAddress = *debugListenAddressArg
	)

	log := xlog.NewLogger(os.Stderr)

	if debugListenAddress != "" {
		go func() {
			err := http.ListenAndServe(debugListenAddress, http.DefaultServeMux)
			if err != nil {
				log.Errorf("could not start debug listen server at '%s': %v", debugListenAddress, err)
			}
		}()
	}

	shardTimeRanges := result.ShardTimeRanges{}

	blockSizeLen, err := time.ParseDuration(blockSize)
	if err != nil {
		log.Fatalf("could not parse block size '%s': %v", blockSize, err)
	}

	retentionLen, err := time.ParseDuration(bootstrapRetention)
	if err != nil {
		log.Fatalf("could not parse retention '%s': %v", bootstrapRetention, err)
	}

	now := time.Now()
	end := now.Truncate(blockSizeLen).Add(blockSizeLen)    // exclusive
	start := now.Truncate(blockSizeLen).Add(-retentionLen) // inclusive

	var shardsAll []uint32
	if strings.TrimSpace(shards) != "" {
		for _, shard := range strings.Split(shards, ",") {
			shard = strings.TrimSpace(shard)
			if shard == "" {
				continue
			}
			value, err := strconv.Atoi(shard)
			if err != nil {
				log.Fatalf("could not parse shard '%s': %v", shard, err)
			}
			rng := xtime.Range{Start: start, End: end}
			shardTimeRanges[uint32(value)] = xtime.NewRanges().AddRange(rng)
			shardsAll = append(shardsAll, uint32(value))
		}
	} else if shardsCount > 0 {
		for i := uint32(0); i < uint32(shardsCount); i++ {
			rng := xtime.Range{Start: start, End: end}
			shardTimeRanges[i] = xtime.NewRanges().AddRange(rng)
			shardsAll = append(shardsAll, i)
		}
	} else {
		flag.Usage()
		os.Exit(1)
	}

	log.WithFields(
		xlog.NewLogField("pathPrefix", pathPrefix),
		xlog.NewLogField("namespace", namespace),
		xlog.NewLogField("shards", shardsAll),
	).Infof("configured")

	instrumentOpts := instrument.NewOptions().
		SetLogger(log)

	retentionOpts := retention.NewOptions().
		SetBlockSize(blockSizeLen).
		SetRetentionPeriod(retentionLen)

	blockOpts := block.NewOptions()

	encoderPool := encoding.NewEncoderPool(nil)
	iteratorPool := encoding.NewReaderIteratorPool(nil)
	multiIteratorPool := encoding.NewMultiReaderIteratorPool(nil)
	segmentReaderPool := xio.NewSegmentReaderPool(nil)

	encodingOpts := encoding.NewOptions().
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetBytesPool(blockOpts.BytesPool()).
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

	segmentReaderPool.Init()

	blockPool := block.NewDatabaseBlockPool(nil)
	blockPool.Init(func() block.DatabaseBlock {
		return block.NewDatabaseBlock(time.Time{}, ts.Segment{}, blockOpts)
	})

	blockOpts = blockOpts.
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(iteratorPool).
		SetMultiReaderIteratorPool(multiIteratorPool).
		SetDatabaseBlockPool(blockPool).
		SetSegmentReaderPool(segmentReaderPool)

	resultOpts := result.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRetentionOptions(retentionOpts).
		SetDatabaseBlockOptions(blockOpts)

	fsOpts := fs.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRetentionOptions(retentionOpts).
		SetFilePathPrefix(pathPrefix)

	commitLogOpts := commitlog.NewOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRetentionOptions(retentionOpts).
		SetFilesystemOptions(fsOpts)

	opts := commitlogsrc.NewOptions().
		SetResultOptions(resultOpts).
		SetCommitLogOptions(commitLogOpts)

	log.Infof("bootstrapping")

	// Don't bootstrap anything else
	next := bootstrapper.NewNoOpAllBootstrapper()

	source := commitlogsrc.NewCommitLogBootstrapper(opts, next)

	nsID := ts.StringID(namespace)
	runOpts := bootstrap.NewRunOptions().
		SetIncremental(false)
	result, err := source.Bootstrap(nsID, shardTimeRanges, runOpts)
	if err != nil {
		log.Fatalf("failed to bootstrap: %v", err)
	}

	log.WithFields(
		xlog.NewLogField("shardResults", len(result.ShardResults())),
		xlog.NewLogField("unfulfilled", len(result.Unfulfilled())),
	).Infof("bootstrapped")

	for shard, result := range result.ShardResults() {
		log.WithFields(
			xlog.NewLogField("shard", shard),
			xlog.NewLogField("series", len(result.AllSeries())),
		).Infof("shard result")
	}
}
