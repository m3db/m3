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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/pborman/getopt"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
)

func main() {
	var (
		path         = getopt.StringLong("path", 'p', "", "file path [e.g. /var/lib/m3db/commitlogs/commitlog-0-161023.db]")
		idFilter     = getopt.StringLong("id-filter", 'f', "", "ID Contains Filter (optional)")
		idSizeFilter = getopt.Uint32Long("id-size-filter", 's', 0, "ID Size (bytes) Filter (optional)")
		action       = getopt.StringLong("action", 'a', "", "Action [print,summary]. Defaults to 'print'")
		top          = getopt.Uint32Long("top", 't', 0, "Print out only top N IDs")
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

	opts := commitlog.NewReaderOptions(commitlog.NewOptions(), false)
	reader := commitlog.NewReader(opts)

	_, err = reader.Open(*path)
	if err != nil {
		logger.Fatalf("unable to open reader: %v", err)
	}

	switch *action {
	case "summary":
		summaryAction(reader, logger, idFilter, idSizeFilter, top)
	default:
		printAction(reader, logger, idFilter, idSizeFilter)
	}

}

func printAction(reader commitlog.Reader, logger *zap.SugaredLogger, idFilter *string, idSizeFilter *uint32) {

	var (
		entryCount          uint32
		annotationSizeTotal uint64
		start               = time.Now()
	)

	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			logger.Fatalf("err reading commitlog: %v", err)
		}

		series := entry.Series
		if *idFilter != "" && !strings.Contains(series.ID.String(), *idFilter) {
			continue
		}
		if *idSizeFilter != 0 && len(series.ID.Bytes()) < int(*idSizeFilter) {
			continue
		}

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

	if err := reader.Close(); err != nil {
		log.Fatalf("unable to close reader: %v", err)
	}

	fmt.Printf("\nRunning time: %s\n", runTime)                          // nolint: forbidigo
	fmt.Printf("%d entries read\n", entryCount)                          // nolint: forbidigo
	fmt.Printf("Total annotation size: %d bytes\n", annotationSizeTotal) // nolint: forbidigo
}

func summaryAction(reader commitlog.Reader, logger *zap.SugaredLogger, idFilter *string, idSizeFilter *uint32, top *uint32) {
	var (
		entryCount        uint32
		start             = time.Now()
		datapointCount    = map[ident.ID]uint32{}
		totalIDSize       uint64
		earliestDatapoint xtime.UnixNano
		oldestDatapoint   xtime.UnixNano
	)

	for {
		entry, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			logger.Fatalf("err reading commitlog: %v", err)
		}

		series := entry.Series
		if *idFilter != "" && !strings.Contains(series.ID.String(), *idFilter) {
			continue
		}
		if *idSizeFilter != 0 && len(series.ID.Bytes()) < int(*idSizeFilter) {
			continue
		}

		if earliestDatapoint == 0 || earliestDatapoint > entry.Datapoint.TimestampNanos {
			earliestDatapoint = entry.Datapoint.TimestampNanos
		}
		if oldestDatapoint == 0 || oldestDatapoint < entry.Datapoint.TimestampNanos {
			oldestDatapoint = entry.Datapoint.TimestampNanos
		}
		datapointCount[series.ID] = datapointCount[series.ID] + 1
		entryCount++
	}

	runTime := time.Since(start)

	if err := reader.Close(); err != nil {
		log.Fatalf("unable to close reader: %v", err)
	}

	fmt.Printf("\nRunning time: %s\n", runTime)                                              // nolint: forbidigo
	fmt.Printf("%d entries read\n", entryCount)                                              // nolint: forbidigo
	fmt.Printf("time range [%s:%s]\n", earliestDatapoint.String(), oldestDatapoint.String()) // nolint: forbidigo

	frequenciesArr := IDPairList{}
	sizeArr := IDPairList{}
	for k, v := range datapointCount {
		keySize := len(k.Bytes())
		totalIDSize += uint64(keySize)
		frequenciesArr = append(frequenciesArr, IDPair{Key: k, Value: v})
		sizeArr = append(sizeArr, IDPair{Key: k, Value: uint32(keySize)})
	}

	sort.Sort(sort.Reverse(frequenciesArr))
	sort.Sort(sort.Reverse(sizeArr))

	fmt.Printf("total ID size: %d bytes\n", totalIDSize)
	fmt.Printf("total distinct number of IDs %d \n", len(datapointCount))

	limit := len(frequenciesArr)
	if *top > 0 {
		limit = int(*top)
	}
	fmt.Printf("key datapoint count: \n") // nolint: forbidigo
	for i := 0; i < limit; i++ {
		pair := frequenciesArr[i]
		fmt.Printf("%-10d %s\n", pair.Value, pair.Key.String()) // nolint: forbidigo
	}

	fmt.Printf("key sizes(bytes): \n") // nolint: forbidigo
	for i := 0; i < limit; i++ {
		pair := sizeArr[i]
		fmt.Printf("%-10d %s\n", pair.Value, pair.Key.String()) // nolint: forbidigo
	}
}

type IDPair struct {
	Key   ident.ID
	Value uint32
}

type IDPairList []IDPair

func (p IDPairList) Len() int           { return len(p) }
func (p IDPairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p IDPairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
