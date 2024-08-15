// Copyright (c) 2021 Uber Technologies, Inc.
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
	"encoding/base64"
	"fmt"
	"log"
	"os"
	"sort"
	"time"

	"github.com/pborman/getopt"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func main() {
	var (
		path         = getopt.StringLong("path", 'p', "", "file path [e.g. /var/lib/m3db/commitlogs/commitlog-0-161023.db]")
		idFilter     = getopt.StringLong("id-filter", 'f', "", "ID Contains Filter (optional)")
		idSizeFilter = getopt.IntLong("id-size-filter", 's', 0, "ID Size (bytes) Filter (optional)")
		mode         = getopt.StringLong("mode", 'm', "", "Action [print,summary]. Defaults to 'print'")
		top          = getopt.IntLong("top", 't', 0, "Print out only top N IDs")
	)
	getopt.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	logger := rawLogger.Sugar()

	if *path == "" {
		getopt.Usage()
		os.Exit(1)
	}

	reader, err := newFilteringReader(*path, idFilter, idSizeFilter)
	if err != nil {
		logger.Fatalf("unable to open reader: %v", err)
	}
	defer reader.Close()

	switch *mode {
	case "summary":
		err = printSummary(reader, top)
	default:
		err = printMetrics(reader)
	}
	if err != nil {
		logger.Fatalf("error while reading commitlog: %v", err)
	}
}

func printMetrics(reader *filteringReader) error {
	var (
		entryCount          uint32
		annotationSizeTotal uint64
		start               = time.Now()
	)

	for {
		entry, found, err := reader.Read()
		if err != nil {
			return err
		}
		if !found {
			break
		}

		series := entry.Series
		fmt.Printf("{id: %s, dp: %+v, ns: %s, shard: %d", // nolint: forbidigo
			series.ID, entry.Datapoint, entry.Series.Namespace, entry.Series.Shard)
		if len(entry.Annotation) > 0 {
			fmt.Printf(", annotation: %s", // nolint: forbidigo
				base64.StdEncoding.EncodeToString(entry.Annotation))
			annotationSizeTotal += uint64(len(entry.Annotation))
		}
		fmt.Println("}") // nolint: forbidigo

		entryCount++
	}

	runTime := time.Since(start)

	fmt.Printf("\nRunning time: %s\n", runTime)                          // nolint: forbidigo
	fmt.Printf("%d entries read\n", entryCount)                          // nolint: forbidigo
	fmt.Printf("Total annotation size: %d bytes\n", annotationSizeTotal) // nolint: forbidigo
	return nil
}

func printSummary(reader *filteringReader, top *int) error {
	var (
		entryCount        uint32
		start             = time.Now()
		datapointCount    = map[ident.ID]uint32{}
		totalIDSize       uint64
		earliestDatapoint xtime.UnixNano
		oldestDatapoint   xtime.UnixNano
	)

	for {
		entry, found, err := reader.Read()
		if err != nil {
			return err
		}
		if !found {
			break
		}
		dp := entry.Datapoint

		if earliestDatapoint == 0 || earliestDatapoint > dp.TimestampNanos {
			earliestDatapoint = dp.TimestampNanos
		}
		if oldestDatapoint == 0 || oldestDatapoint < dp.TimestampNanos {
			oldestDatapoint = dp.TimestampNanos
		}

		datapointCount[entry.Series.ID]++

		entryCount++
	}

	runTime := time.Since(start)

	fmt.Printf("\nRunning time: %s\n", runTime)                                              // nolint: forbidigo
	fmt.Printf("%d entries read\n", entryCount)                                              // nolint: forbidigo
	fmt.Printf("time range [%s:%s]\n", earliestDatapoint.String(), oldestDatapoint.String()) // nolint: forbidigo

	datapointCountArr := idPairs{}
	sizeArr := idPairs{}
	for ID, count := range datapointCount {
		IDSize := len(ID.Bytes())
		totalIDSize += uint64(IDSize)
		datapointCountArr = append(datapointCountArr, idPair{ID: ID, Value: count})
		sizeArr = append(sizeArr, idPair{ID: ID, Value: uint32(IDSize)})
	}

	sort.Sort(sort.Reverse(datapointCountArr))
	sort.Sort(sort.Reverse(sizeArr))

	fmt.Printf("total ID size: %d bytes\n", totalIDSize)                  // nolint: forbidigo
	fmt.Printf("total distinct number of IDs %d \n", len(datapointCount)) // nolint: forbidigo

	limit := len(datapointCountArr)
	if *top > 0 && *top < limit {
		limit = *top
	}
	fmt.Printf("ID datapoint counts: \n") // nolint: forbidigo
	for i := 0; i < limit; i++ {
		pair := datapointCountArr[i]
		fmt.Printf("%-10d %s\n", pair.Value, pair.ID.String()) // nolint: forbidigo
	}

	fmt.Printf("ID sizes(bytes): \n") // nolint: forbidigo
	for i := 0; i < limit; i++ {
		pair := sizeArr[i]
		fmt.Printf("%-10d %s\n", pair.Value, pair.ID.String()) // nolint: forbidigo
	}

	return nil
}

type idPair struct {
	ID    ident.ID
	Value uint32
}

type idPairs []idPair

func (p idPairs) Len() int           { return len(p) }
func (p idPairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p idPairs) Less(i, j int) bool { return p[i].Value < p[j].Value }
