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
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/index/convert"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/ident"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

const (
	snapshotType = "snapshot"
	flushType    = "flush"
)

type benchmarkMode uint8

const (
	// benchmarkNone prints the data read to the standard output and does not measure performance.
	benchmarkNone benchmarkMode = iota

	// benchmarkSeries benchmarks time series read performance (skipping datapoint decoding).
	benchmarkSeries

	// benchmarkDatapoints benchmarks series read, including datapoint decoding.
	benchmarkDatapoints
)

func main() {
	var (
		optPathPrefix  = getopt.StringLong("path-prefix", 'p', "", "Path prefix [e.g. /var/lib/m3db]")
		optNamespace   = getopt.StringLong("namespace", 'n', "default", "Namespace [e.g. metrics]")
		optShard       = getopt.Uint32Long("shard", 's', 0, "Shard [expected format uint32]")
		optBlockstart  = getopt.Int64Long("block-start", 'b', 0, "Block Start Time [in nsec]")
		volume         = getopt.Int64Long("volume", 'v', 0, "Volume number")
		fileSetTypeArg = getopt.StringLong("fileset-type", 't', flushType, fmt.Sprintf("%s|%s", flushType, snapshotType))
		idFilter       = getopt.StringLong("id-filter", 'f', "", "ID Contains Filter (optional)")
		benchmark      = getopt.StringLong(
			"benchmark", 'B', "", "benchmark mode (optional), [series|datapoints]")
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

	var benchMode benchmarkMode
	switch *benchmark {
	case "":
	case "series":
		benchMode = benchmarkSeries
	case "datapoints":
		benchMode = benchmarkDatapoints
	default:
		log.Fatalf("unknown benchmark type: %s", *benchmark)
	}

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	fsOpts := fs.NewOptions().SetFilePathPrefix(*optPathPrefix)

	encodingOpts := encoding.NewOptions().SetBytesPool(bytesPool)

	var (
		seriesCount         = 0
		datapointCount      = 0
		annotationSizeTotal uint64
		start               = time.Now()

		metricsMap = make(map[string]struct{})
	)

	fmt.Printf("Reading shards [0-%d)\n", *optShard)

	for shardID := uint32(0); shardID < *optShard; shardID++ {

		reader, err := fs.NewReader(bytesPool, fsOpts)
		if err != nil {
			log.Fatalf("could not create new reader for shard %d: %v", shardID, err)
		}

		openOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   ident.StringID(*optNamespace),
				Shard:       shardID,
				BlockStart:  time.Unix(0, *optBlockstart),
				VolumeIndex: int(*volume),
			},
			FileSetType:      fileSetType,
			StreamingEnabled: true,
		}

		err = reader.Open(openOpts)
		if err != nil {
			log.Fatalf("unable to open reader: %v", err)
		}

		nameTag := ident.BytesID("__name__")
		//jobTag := ident.BytesID("job")

		for {
			entry, err := reader.StreamingRead()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("err reading metadata: %v", err)
			}

			id := entry.ID

			if *idFilter != "" && !strings.Contains(id.String(), *idFilter) {
				continue
			}

			hasAnnotation := false
			iter := m3tsz.NewReaderIterator(xio.NewBytesReader64(entry.Data), true, encodingOpts)
			if iter.Next() {
				_, _, annotation := iter.Current()
				hasAnnotation = len(annotation) > 0
			}
			if err := iter.Err(); err != nil {
				log.Fatalf("unable to iterate original data: %v", err)
			}
			iter.Close()

			if hasAnnotation {
				continue
			}

			doc, err := convert.FromSeriesIDAndEncodedTags(id, entry.EncodedTags)
			if err != nil {
				log.Fatalf("err decoding tags: %v", err)
			}
			var name []byte
			for _, field := range doc.Fields {
				if bytes.Equal(field.Name, nameTag) {
					name = field.Value
				}
				//if bytes.Equal(field.Name, jobTag) {
				//	job = field.Value
				//}
			}

			res := fmt.Sprintf("%s", name)
			metricsMap[res] = struct{}{}

			seriesCount++
		}

		if benchMode != benchmarkNone {
			runTime := time.Since(start)
			fmt.Printf("Running time: %s\n", runTime)     // nolint: forbidigo
			fmt.Printf("\n%d series read\n", seriesCount) // nolint: forbidigo
			if runTime > 0 {
				fmt.Printf("(%.2f series/second)\n", float64(seriesCount)/runTime.Seconds()) // nolint: forbidigo
			}

			if benchMode == benchmarkDatapoints {
				fmt.Printf("\n%d datapoints decoded\n", datapointCount) // nolint: forbidigo
				if runTime > 0 {
					fmt.Printf("(%.2f datapoints/second)\n", float64(datapointCount)/runTime.Seconds()) // nolint: forbidigo
				}

				fmt.Printf("\nTotal annotation size: %d bytes\n", annotationSizeTotal) // nolint: forbidigo
			}
		}

		if err := reader.Close(); err != nil {
			log.Fatalf("unable to close reader: %v", err)
		}
	}

	metrics := make([]string, 0)
	for m := range metricsMap {
		metrics = append(metrics, m)
	}

	sort.Strings(metrics)
	for _, m := range metrics {
		fmt.Println(m)
	}
}
