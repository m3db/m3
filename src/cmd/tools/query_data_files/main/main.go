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
	"bytes"
	"fmt"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/streaming/downsamplers"
	"github.com/m3db/m3/src/dbnode/streaming/iterators"
	"io"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

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
		optShard       = getopt.Uint32Long("shard", 's', 0, "Shard [expected format uint32]")
		optBlockstart  = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		optTilesize    = getopt.Int64Long("tile-size", 't', 5, "Block Start Time [in min]")
		volume         = getopt.Int64Long("volume", 'v', 0, "Volume number")
		concurrency    = getopt.Int64Long("concurrency", 'c', int64(runtime.NumCPU()), "Concurrent iteration count")
		fileSetTypeArg = getopt.StringLong("fileset-type", 'f', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))

		iterationCount = getopt.IntLong("iterations", 'i', 50, "Concurrent iteration count")
	)
	getopt.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	log := rawLogger.Sugar()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optShard < 0 ||
		*optBlockstart <= 0 ||
		*optTilesize <= 0 ||
		*volume < 0 ||
		*concurrency < 1 ||
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
		encodingOpts = encoding.NewOptions().SetBytesPool(bytesPool)
		fsOpts       = fs.NewOptions().SetFilePathPrefix(*optPathPrefix)

		iterations = *iterationCount
		c          = int(*concurrency)

		readStart = time.Now()
	)

	for iteration := 0; iteration < iterations; iteration++ {
		fmt.Println("Running iteration", iteration)

		reader, err := fs.NewReader(bytesPool, fsOpts)
		if err != nil {
			log.Fatalf("could not create new reader: %v", err)
		}

		openOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   ident.StringID(*optNamespace),
				Shard:       *optShard,
				BlockStart:  time.Unix(0, *optBlockstart),
				VolumeIndex: int(*volume),
			},
			FileSetType: fileSetType,
		}

		err = reader.Open(openOpts)
		if err != nil {
			log.Fatalf("unable to open reader: %v", err)
		}

		var (
			step  = time.Duration(*optTilesize) * time.Minute
			start = xtime.UnixNano(*optBlockstart)
			tags  = make([]string, initValLength)
			vals  = make([]float64, 0, initValLength)
		)

		defer reader.Close()

		bytesReader := bytes.NewReader(nil)
		dataPointIter := m3tsz.NewReaderIterator(bytesReader, m3tsz.DefaultIntOptimizationEnabled, encodingOpts)
		downsampler := downsamplers.NewSumDownsampler()

		tags = tags[:0]

		for {
			id, tags, data, _, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("error from Reader: ", err)
				return
			}

			data.IncRef()
			bytesReader.Reset(data.Bytes())
			dataPointIter.Reset(bytesReader, nil)
			aggregatedIter := iterators.NewStepIterator(dataPointIter, start, step, downsampler)

			vals = vals[:0]

			for aggregatedIter.Next() {
				dp, _, _ := aggregatedIter.Current()
				v := dp.Value

				vals = append(vals, v)
			}

			aggregatedIter.Close()

			id.Finalize()
			tags.Close()
			data.DecRef()
			data.Finalize()
		}

		if err := dataPointIter.Err(); err != nil {
			fmt.Println("series error:", err)
		}

		dataPointIter.Close()
	}

	fmt.Printf("Using step iterator\nIterations: %d\nConcurrency: %d\nTook: %v\n",
		iterations, c, time.Since(readStart))
}
