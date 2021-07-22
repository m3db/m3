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
	"fmt"
	"io"
	iofs "io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/pborman/getopt"
	"go.uber.org/zap"
)

var (
	checkpointPattern = regexp.MustCompile(`(\w+)/([0-9]+)/fileset-([0-9]+)-([0-9]+)-checkpoint.db`)
)

func main() {
	var (
		optSrcPathPrefix = getopt.StringLong("src-path", 's', "", "Source path prefix [e.g. /temp/lib/m3db]")
		optDstPathPrefix = getopt.StringLong("dst-path", 'd', "", "Destination path prefix [e.g. /var/lib/m3db]")
		optBlockUntil    = getopt.Int64Long("block-until", 'b', 0, "Block Until Time [in nsec]")
		optShards        = getopt.Uint32Long("src-shards", 'h', 0, "Original (source) number of shards")
		optFactor        = getopt.IntLong("factor", 'f', 0, "Integer factor to increase the number of shards by")
	)
	getopt.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	logger := rawLogger.Sugar()

	if *optSrcPathPrefix == "" ||
		*optDstPathPrefix == "" ||
		*optSrcPathPrefix == *optDstPathPrefix ||
		*optBlockUntil <= 0 ||
		*optShards <= 0 ||
		*optFactor <= 0 {
		getopt.Usage()
		os.Exit(1)
	}

	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	srcFsOpts := fs.NewOptions().SetFilePathPrefix(*optSrcPathPrefix)
	dstFsOpts := fs.NewOptions().SetFilePathPrefix(*optDstPathPrefix)

	reader, err := fs.NewReader(bytesPool, srcFsOpts)
	if err != nil {
		logger.Fatalf("could not create new reader: %v", err)
	}

	writers := make([]fs.StreamingWriter, *optFactor)
	for i := range writers {
		writers[i], err = fs.NewStreamingWriter(dstFsOpts)
		if err != nil {
			logger.Fatalf("could not create writer, %v", err)
		}
	}

	hashFn := sharding.DefaultHashFn(int(*optShards) * (*optFactor))

	start := time.Now()

	srcDataPath := *optSrcPathPrefix + "/data"
	if err := filepath.WalkDir(srcDataPath, func(path string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), "-checkpoint.db") {
			return err
		}
		logger.Info(path)
		pathParts := checkpointPattern.FindStringSubmatch(path)
		if len(pathParts) != 5 {
			return fmt.Errorf("failed to parse path %s", path)
		}

		var (
			namespace     = pathParts[1]
			shard, _      = strconv.Atoi(pathParts[2])
			blockStart, _ = strconv.Atoi(pathParts[3])
			volume, _     = strconv.Atoi(pathParts[4])
		)

		_, err = splitFileSet(reader, writers, hashFn, *optShards, *optFactor, logger, namespace, uint32(shard), xtime.UnixNano(blockStart), volume)
		return err
	}); err != nil {
		logger.Fatalf("unable to walk the source dir: %v", err)
	}

	runTime := time.Since(start)
	fmt.Printf("Running time: %s\n", runTime) // nolint: forbidigo
	//fmt.Printf("\n%d series read\n", seriesCount) // nolint: forbidigo
}

func splitFileSet(
	reader fs.DataFileSetReader,
	writers []fs.StreamingWriter,
	hashFn sharding.HashFn,
	srcNumShards uint32,
	factor int,
	logger *zap.SugaredLogger,
	namespace string,
	shard uint32,
	blockStart xtime.UnixNano,
	volume int,
) (int, error) {
	if shard >= srcNumShards {
		return 0, fmt.Errorf("unexpected source shard ID %d (must be under %d)", shard, srcNumShards)
	}

	readOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   ident.StringID(namespace),
			Shard:       shard,
			BlockStart:  blockStart,
			VolumeIndex: volume,
		},
		FileSetType:      persist.FileSetFlushType,
		StreamingEnabled: true,
	}

	err := reader.Open(readOpts)
	if err != nil {
		logger.Fatalf("unable to open reader: %v", err)
	}

	plannedRecordsCount := uint(reader.Entries() / factor)
	if plannedRecordsCount == 0 {
		plannedRecordsCount = 1
	}

	for i := range writers {
		writeOpts := fs.StreamingWriterOpenOptions{
			NamespaceID:         ident.StringID(namespace),
			ShardID:             srcNumShards*uint32(i) + shard,
			BlockStart:          blockStart,
			BlockSize:           reader.Status().BlockSize,
			VolumeIndex:         volume,
			PlannedRecordsCount: plannedRecordsCount,
		}
		if err := writers[i].Open(writeOpts); err != nil {
			return 0, err
		}
	}

	seriesCount := 0
	dataHolder := make([][]byte, 1)
	for {
		entry, err := reader.StreamingRead()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("read error: %v", err)
		}

		newShardID := hashFn(entry.ID)
		if newShardID%srcNumShards != shard {
			return 0, fmt.Errorf("mismatched shards, %d to %d", shard, newShardID)
		}
		writer := writers[newShardID/srcNumShards]

		dataHolder[0] = entry.Data
		if err := writer.WriteAll(entry.ID, entry.EncodedTags, dataHolder, entry.DataChecksum); err != nil {
			return 0, err
		}

		seriesCount++
	}

	for i := range writers {
		if err := writers[i].Close(); err != nil {
			return 0, err
		}
	}

	if err := reader.Close(); err != nil {
		return 0, err
	}

	return seriesCount, nil
}
