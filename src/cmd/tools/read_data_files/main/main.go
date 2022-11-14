// Copyright (c) 2018 Uber Technologies, Inc.
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
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/x/xio"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

const (
	snapshotType = "snapshot"
	flushType    = "flush"

	allShards = -1
)

type benchmarkMode uint8

const (
	// benchmarkNone prints out the read performance
	benchmarkNone benchmarkMode = iota

	// benchmarkSeries outputs only the time series and their sum over the data block.
	benchmarkSeries

	// benchmarkDatapoints outputs all datapoints' timestamps and values.
	benchmarkDatapoints
)

func main() {
	var (
		optPathPrefix = getopt.StringLong("path-prefix", 'p', "/var/lib/m3db", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace  = getopt.StringLong("namespace", 'n', "default", "Namespace [e.g. metrics]")
		optShard      = getopt.IntLong("shard", 's', allShards,
			fmt.Sprintf("Shard [expected format uint32], or %v for all shards in the directory", allShards))
		optBlockstart  = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		volume         = getopt.Int64Long("volume", 'v', 0, "Volume number")
		fileSetTypeArg = getopt.StringLong("fileset-type", 't', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))
		idFilter       = getopt.StringLong("id-filter", 'f', "", "ID Contains Filter (optional)")
		benchmark      = getopt.StringLong(
			"benchmark", 'B', "series", "benchmark mode (optional), [series|datapoints]")
	)
	getopt.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	log := rawLogger.Sugar()

	if *optPathPrefix == "" ||
		*optNamespace == "" ||
		*optShard < allShards ||
		*optBlockstart <= 0 ||
		*volume < 0 ||
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

	// instead of doing benchmark, the -B option specifiy the output data format
	// "": outputs no data but benchmark results
	// series: only outputs the number of data points + sum of all values (if any)
	// datapoints: outputs a list of timestamps + values and in comma separated
	var benchMode benchmarkMode
	switch *benchmark {
	case "":
		benchMode = benchmarkNone
	case "series":
		benchMode = benchmarkSeries
		fmt.Println("shard\tid\tdatapoints\tsum\tstartTimeInSec\tendTimeInSec")
	case "datapoints":
		benchMode = benchmarkDatapoints
		fmt.Println("shard\tid\tdatapoints\ttimestamps\tvalues")
	default:
		log.Fatalf("unknown benchmark type: %s", *benchmark)
	}

	// Not using bytes pool with streaming reads/writes to avoid the fixed memory overhead.
	var bytesPool pool.CheckedBytesPool
	encodingOpts := encoding.NewOptions().SetBytesPool(bytesPool)

	fsOpts := fs.NewOptions().SetFilePathPrefix(*optPathPrefix)

	shards := []uint32{uint32(*optShard)}
	if *optShard == allShards {
		shards, err = getShards(*optPathPrefix, fileSetType, *optNamespace)
		if err != nil {
			log.Fatalf("failed to resolve shards: %v", err)
		}
	}

	reader, err := fs.NewReader(bytesPool, fsOpts)
	if err != nil {
		log.Fatalf("could not create new reader: %v", err)
	}

	for _, shard := range shards {
		var (
			seriesCount    = 0
			datapointCount = 0
			start          = time.Now()
		)

		openOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   ident.StringID(*optNamespace),
				Shard:       shard,
				BlockStart:  xtime.UnixNano(*optBlockstart),
				VolumeIndex: int(*volume),
			},
			FileSetType:      fileSetType,
			StreamingEnabled: true,
		}

		err = reader.Open(openOpts)
		if err != nil {
			log.Fatalf("unable to open reader for shard %v: %v", shard, err)
		}

		for {
			entry, err := reader.StreamingRead()
			if xerrors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				log.Fatalf("err reading metadata: %v", err)
			}

			var (
				id   = entry.ID
				data = entry.Data
			)

			if *idFilter != "" && !strings.Contains(id.String(), *idFilter) {
				continue
			}

			startTime := xtime.ToUnixNano(time.Now())
			endTime := xtime.FromSeconds(0)
			datapointsPerSeries := 0
			sumPerSeries := 0.0
			var timestamps, values strings.Builder
			timestamps.WriteByte('t')
			values.WriteByte('v')

			seriesCount++
			iter := m3tsz.NewReaderIterator(xio.NewBytesReader64(data), true, encodingOpts)
			if benchMode != benchmarkNone {
				fmt.Printf("%d\t%s\t", shard, id.String())
			}
			for iter.Next() {
				datapointCount++

				dp, _, _ := iter.Current()
				datapointsPerSeries++
				if benchMode == benchmarkSeries {
					sumPerSeries += dp.Value
					startTime = xtime.MinUnixNano(startTime, dp.TimestampNanos)
					endTime = xtime.MaxUnixNano(endTime, dp.TimestampNanos)
				} else if benchMode == benchmarkDatapoints {
					timestamps.WriteString(fmt.Sprintf(",%d", dp.TimestampNanos/1e9))
					values.WriteString(fmt.Sprintf(",%.2f", dp.Value))
				}
			}
			switch benchMode {
			case benchmarkSeries:
				if math.IsNaN(sumPerSeries) {
					fmt.Printf("%d\t\t%d\t%d\n", datapointsPerSeries, startTime.Seconds(), endTime.Seconds())
				} else {
					fmt.Printf("%d\t%.2f\t%d\t%d\n", datapointsPerSeries, sumPerSeries, startTime.Seconds(), endTime.Seconds())
				}
				break
			case benchmarkDatapoints:
				fmt.Printf("%d\t%s\t%s\n", datapointsPerSeries, timestamps.String(), values.String())
				break
			}
			if err := iter.Err(); err != nil {
				log.Fatalf("unable to iterate original data: %v", err)
			}
			iter.Close()
		}

		if seriesCount != reader.Entries() && *idFilter == "" {
			log.Warnf("actual time series count (%d) did not match info file data (%d)",
				seriesCount, reader.Entries())
		}

		if benchMode == benchmarkNone {
			runTime := time.Since(start)
			// csv ouptut, header with shard,series,
			fmt.Printf("%d,%d,%d", shard, seriesCount, datapointCount)
			// elasped_time
			fmt.Printf("%s\n", runTime)
		}
	}

	if err := reader.Close(); err != nil {
		log.Fatalf("unable to close reader: %v", err)
	}
}

func getShards(pathPrefix string, fileSetType persist.FileSetType, namespace string) ([]uint32, error) {
	nsID := ident.StringID(namespace)
	path := fs.NamespaceDataDirPath(pathPrefix, nsID)
	if fileSetType == persist.FileSetSnapshotType {
		path = fs.NamespaceSnapshotsDirPath(pathPrefix, nsID)
	}

	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("failed reading namespace directory: %w", err)
	}

	shards := make([]uint32, 0)
	for _, f := range files {
		if !f.IsDir() {
			continue
		}
		i, err := strconv.Atoi(f.Name())
		if err != nil {
			return nil, fmt.Errorf("failed extracting shard number: %w", err)
		}
		if i < 0 {
			return nil, fmt.Errorf("negative shard number %v", i)
		}
		shards = append(shards, uint32(i))
	}

	return shards, nil
}
