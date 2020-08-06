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

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/arrow/tile"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"net/http"
	_ "net/http/pprof"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

const snapshotType = "snapshot"
const flushType = "flush"
const initValLength = 10

func main() {
	var (
		optPathPrefix  = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace   = getopt.StringLong("namespace", 'n', "", "Namespace [e.g. metrics]")
		optShard       = getopt.Int32Long("shard", 's', -1, "Shard [expected format uint32]")
		optBlockstart  = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		optTilesize    = getopt.Int64Long("tile-size", 't', 5, "Block Start Time [in min]")
		volume         = getopt.Int64Long("volume", 'v', 0, "Volume number")
		fileSetTypeArg = getopt.StringLong("fileset-type", 'f', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))

		iterationCount = getopt.IntLong("iterations", 'i', 50, "Concurrent iteration count")
		optUseArrow    = getopt.Bool('a', "Use arrow")
	)
	getopt.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	log := rawLogger.Sugar()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optBlockstart <= 0 ||
		*optTilesize <= 0 ||
		*volume < 0 ||
		*iterationCount < 1 ||
		(*fileSetTypeArg != snapshotType && *fileSetTypeArg != flushType) {
		getopt.Usage()
		os.Exit(1)
	}

	var fileSetType persist.FileSetType
	switch *fileSetTypeArg {
	case flushType:
		fileSetType = persist.FileSetFlushType
	case snapshotType:
		fileSetType = persist.FileSetSnapshotType
	default:
		log.Fatalf("unknown fileset type: %s", *fileSetTypeArg)
	}

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	var (
		fsOpts = fs.NewOptions().SetFilePathPrefix(*optPathPrefix)

		iterations = *iterationCount
		useArrow   = *optUseArrow
		readStart  = time.Now()
	)

	var shards []uint32
	if *optShard >= 0 {
		shards = append(shards, uint32(*optShard))
	} else {
		for shard := 0; shard < 256; shard++ {
			path := fmt.Sprintf("%s/data/%s/%d", *optPathPrefix, *optNamespace, shard)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				continue
			}
			shards = append(shards, uint32(shard))
		}
	}

	iterPoolPoolOpts := pool.NewObjectPoolOptions().SetSize(1024)
	iterPool := encoding.NewReaderIteratorPool(iterPoolPoolOpts)
	encodingOpts := encoding.NewOptions().SetReaderIteratorPool(iterPool)
	iterPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
	})

	for iteration := 0; iteration < iterations; iteration++ {
		for _, shard := range shards {
			reader, err := fs.NewReader(bytesPool, fsOpts)
			if err != nil {
				log.Fatalf("could not create new reader: %v", err)
			}

			openOpts := fs.DataReaderOpenOptions{
				Identifier: fs.FileSetFileIdentifier{
					Namespace:   ident.StringID(*optNamespace),
					Shard:       shard,
					BlockStart:  time.Unix(0, *optBlockstart),
					VolumeIndex: int(*volume),
				},
				FileSetType:    fileSetType,
				OrderedByIndex: true,
			}

			err = reader.Open(openOpts)
			if err != nil {
				log.Fatalf("unable to open reader: %v", err)
			}

			var (
				frameSize = xtime.UnixNano(*optTilesize) * xtime.UnixNano(time.Minute)
				start     = xtime.UnixNano(*optBlockstart)
			)

			opts := tile.Options{
				FrameSize:          frameSize,
				Start:              start,
				UseArrow:           useArrow,
				ReaderIteratorPool: iterPool,
			}

			crossBlockReader, err := fs.NewCrossBlockReader([]fs.DataFileSetReader{reader})
			if err != nil {
				fmt.Println("error creating CrossBlockReader", err)
				return
			}

			it, err := tile.NewSeriesBlockIterator(crossBlockReader, opts)
			if err != nil {
				fmt.Println("error creating block iterator", err)
				return
			}

			for it.Next() {
				frameIter := it.Current()
				for frameIter.Next() {
					// No-op for now for better benchmarking underlying iters.
				}
			}

			if err := it.Close(); err != nil {
				fmt.Println("iterator close error:", err)
			}

			if err := it.Err(); err != nil {
				fmt.Println("series error:", err)
			}

			if err := reader.Close(); err != nil {
				fmt.Println("reader close error:", err)
			}
		}
	}

	frameSize := time.Duration(*optTilesize) * time.Minute
	if useArrow {
		fmt.Printf("Using arrow buffers\nIterations: %d\nFrameSize: %v\nTook: %v\n",
			iterations, frameSize, time.Since(readStart))
	} else {
		fmt.Printf("Using flat buffers\nIterations: %d\nFrameSize: %v\nTook: %v\n",
			iterations, frameSize, time.Since(readStart))
	}
}
