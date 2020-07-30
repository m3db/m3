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
	"log"
	"math"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/arrow/tile"
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
		arrow          = getopt.Bool('a', "Use arrow")
		optimized      = getopt.Bool('o', "Optimized iteration")
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

		iterations  = *iterationCount
		useArrow    = *arrow
		optimizeSum = *optimized
		c           = int(*concurrency)

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
			frameSize = xtime.UnixNano(*optTilesize) * xtime.UnixNano(time.Minute)
			start     = xtime.UnixNano(*optBlockstart)
			prints    = make([]bool, c)
			tags      = make([][]string, c)
			vals      = make([][]float64, 0, c)
		)

		for i := 0; i < c; i++ {
			vals = append(vals, make([]float64, 0, initValLength))
			tags = append(tags, make([]string, 0, initValLength))
		}

		opts := tile.Options{
			FrameSize:    frameSize,
			Start:        start,
			Concurrency:  c,
			UseArrow:     useArrow,
			EncodingOpts: encodingOpts,
		}

		it, err := tile.NewSeriesBlockIterator(reader, opts)
		if err != nil {
			fmt.Println("error creating block iterator", err)
			return
		}

		defer func() {
			if err := it.Close(); err != nil {
				fmt.Println("iterator close error:", err)
			}
		}()

		i := 0
		printNonZero := func() {
			for j := range prints {
				if prints[j] {
					prints[j] = false
					// idx := (i-1)*c + j
					// fmt.Printf("%d : %v\n", idx, vals[j])
					// fmt.Printf("%v\n", tags[j])
				}
			}
		}

		var wg sync.WaitGroup
		for it.Next() {
			printNonZero()
			for i := range vals {
				vals[i] = vals[i][:0]
			}

			for i := range tags {
				tags[i] = tags[i][:0]
			}

			frameIters := it.Current()
			for j, frameIter := range frameIters {
				// NB: capture loop variables.
				j, frameIter := j, frameIter
				wg.Add(1)
				go func() {
					for frameIter.Next() {
						frame := frameIter.Current()
						var v float64
						if summary := frame.Summary(); optimizeSum && summary.Valid() {
							v = summary.Sum()
						} else {
							v = frame.Sum()
						}

						if v != 0 && !math.IsNaN(v) {
							prints[j] = true
						}

						vals[j] = append(vals[j], v)
						// ts := frame.Tags()
						// sep := fmt.Sprintf("ID: %s\ntags:", frame.ID().String())
						// tags[j] = append(tags[j], sep)
						// for ts.Next() {
						// 	tag := ts.Current()
						// 	t := fmt.Sprintf("%s:%s", tag.Name.String(), tag.Value.String())
						// 	tags[j] = append(tags[j], t)
						// }

						// unit, single := frame.Units().SingleValue()
						// annotation, annotationSingle := frame.Annotations().SingleValue()
						// meta := fmt.Sprintf("\nunit: %v, single: %v\nannotation: %v, single: %v",
						// 	unit, single, annotation, annotationSingle)
						// tags[j] = append(tags[j], meta)
					}

					if err := frameIter.Err(); err != nil {
						panic(fmt.Sprint("frame error:", err))
					}

					wg.Done()
				}()
			}

			i++
			wg.Wait()
		}

		printNonZero()
		if err := it.Err(); err != nil {
			fmt.Println("series error:", err)
		}
	}

	frameSize := time.Duration(*optTilesize) * time.Minute
	if useArrow {
		fmt.Printf("\nUsing arrow buffers\nIterations: %d\nConcurrency: %d"+
			"\nOptimizing sum: %v\nTilesize: %v\nTook: %v\n",
			iterations, c, optimizeSum, frameSize, time.Since(readStart))
	} else {
		fmt.Printf("\nUsing flat buffers\nIterations: %d\nConcurrency: %d"+
			"\nOptimizing sum: %v\nTilesize: %v\nTook: %v\n",
			iterations, c, optimizeSum, frameSize, time.Since(readStart))
	}
}
