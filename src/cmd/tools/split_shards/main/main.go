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
	"bytes"
	"errors"
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

	"github.com/pborman/getopt"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/sharding"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

var checkpointPattern = regexp.MustCompile(`/data/(\w+)/([0-9]+)/fileset-([0-9]+)-([0-9]+)-checkpoint.db$`)

func main() {
	var (
		optSrcPath       = getopt.StringLong("src-path", 's', "", "Source path [e.g. /temp/lib/m3db/data]")
		optDstPathPrefix = getopt.StringLong("dst-path", 'd', "", "Destination path prefix [e.g. /var/lib/m3db/data]")
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

	if *optSrcPath == "" ||
		*optDstPathPrefix == "" ||
		*optSrcPath == *optDstPathPrefix ||
		*optBlockUntil <= 0 ||
		*optShards == 0 ||
		*optFactor <= 0 {
		getopt.Usage()
		os.Exit(1)
	}

	var (
		srcFilesetLocation = dropDataSuffix(*optSrcPath)
		dstFilesetLocation = dropDataSuffix(*optDstPathPrefix)

		srcFsOpts = fs.NewOptions().SetFilePathPrefix(srcFilesetLocation)
		dstFsOpts = fs.NewOptions().SetFilePathPrefix(dstFilesetLocation)
	)

	// Not using bytes pool with streaming reads/writes to avoid the fixed memory overhead.
	srcReader, err := fs.NewReader(nil, srcFsOpts)
	if err != nil {
		logger.Fatalf("could not create srcReader: %+v", err)
	}

	dstReaders := make([]fs.DataFileSetReader, *optFactor)
	dstWriters := make([]fs.StreamingWriter, *optFactor)
	for i := range dstWriters {
		dstReaders[i], err = fs.NewReader(nil, dstFsOpts)
		if err != nil {
			logger.Fatalf("could not create dstReader, %+v", err)
		}
		dstWriters[i], err = fs.NewStreamingWriter(dstFsOpts)
		if err != nil {
			logger.Fatalf("could not create writer, %+v", err)
		}
	}

	hashFn := sharding.DefaultHashFn(int(*optShards) * (*optFactor))

	start := time.Now()

	if err := filepath.WalkDir(*optSrcPath, func(path string, d iofs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), "-checkpoint.db") {
			return err
		}
		fmt.Printf("%s - %s\n", time.Now().Local(), path) // nolint: forbidigo
		pathParts := checkpointPattern.FindStringSubmatch(path)
		if len(pathParts) != 5 {
			return fmt.Errorf("failed to parse path %s", path)
		}

		var (
			namespace        = pathParts[1]
			shard, err2      = strconv.Atoi(pathParts[2])
			blockStart, err3 = strconv.Atoi(pathParts[3])
			volume, err4     = strconv.Atoi(pathParts[4])
		)
		if err = xerrors.FirstError(err2, err3, err4); err != nil {
			return err
		}

		if blockStart >= int(*optBlockUntil) {
			fmt.Println(" - skip (too recent)") // nolint: forbidigo
			return nil
		}

		if err = splitFileSet(
			srcReader, dstWriters, hashFn, *optShards, *optFactor, namespace, uint32(shard),
			xtime.UnixNano(blockStart), volume); err != nil {
			if strings.Contains(err.Error(), "no such file or directory") {
				fmt.Println(" - skip (incomplete fileset)") // nolint: forbidigo
				return nil
			}
			return err
		}

		err = verifySplitShards(
			srcReader, dstReaders, hashFn, *optShards, namespace, uint32(shard),
			xtime.UnixNano(blockStart), volume)
		if err != nil && strings.Contains(err.Error(), "no such file or directory") {
			return nil
		}
		return err
	}); err != nil {
		logger.Fatalf("unable to walk the source dir: %+v", err)
	}

	runTime := time.Since(start)
	fmt.Printf("Running time: %s\n", runTime) // nolint: forbidigo
}

func splitFileSet(
	srcReader fs.DataFileSetReader,
	dstWriters []fs.StreamingWriter,
	hashFn sharding.HashFn,
	srcNumShards uint32,
	factor int,
	namespace string,
	srcShard uint32,
	blockStart xtime.UnixNano,
	volume int,
) error {
	if srcShard >= srcNumShards {
		return fmt.Errorf("unexpected source shard ID %d (must be under %d)", srcShard, srcNumShards)
	}

	readOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   ident.StringID(namespace),
			Shard:       srcShard,
			BlockStart:  blockStart,
			VolumeIndex: volume,
		},
		FileSetType:      persist.FileSetFlushType,
		StreamingEnabled: true,
	}

	err := srcReader.Open(readOpts)
	if err != nil {
		return fmt.Errorf("unable to open srcReader: %w", err)
	}

	plannedRecordsCount := uint(srcReader.Entries() / factor)
	if plannedRecordsCount == 0 {
		plannedRecordsCount = 1
	}

	for i := range dstWriters {
		writeOpts := fs.StreamingWriterOpenOptions{
			NamespaceID:         ident.StringID(namespace),
			ShardID:             mapToDstShard(srcNumShards, i, srcShard),
			BlockStart:          blockStart,
			BlockSize:           srcReader.Status().BlockSize,
			VolumeIndex:         volume + 1,
			PlannedRecordsCount: plannedRecordsCount,
		}
		if err := dstWriters[i].Open(writeOpts); err != nil {
			return fmt.Errorf("unable to open dstWriters[%d]: %w", i, err)
		}
	}

	dataHolder := make([][]byte, 1)
	for {
		entry, err := srcReader.StreamingRead()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}

		newShardID := hashFn(entry.ID)
		if newShardID%srcNumShards != srcShard {
			return fmt.Errorf("mismatched shards, %d to %d", srcShard, newShardID)
		}
		writer := dstWriters[newShardID/srcNumShards]

		dataHolder[0] = entry.Data
		if err := writer.WriteAll(entry.ID, entry.EncodedTags, dataHolder, entry.DataChecksum); err != nil {
			return err
		}
	}

	for i := range dstWriters {
		if err := dstWriters[i].Close(); err != nil {
			return err
		}
	}

	return srcReader.Close()
}

func verifySplitShards(
	srcReader fs.DataFileSetReader,
	dstReaders []fs.DataFileSetReader,
	hashFn sharding.HashFn,
	srcNumShards uint32,
	namespace string,
	srcShard uint32,
	blockStart xtime.UnixNano,
	volume int,
) error {
	if srcShard >= srcNumShards {
		return fmt.Errorf("unexpected source shard ID %d (must be under %d)", srcShard, srcNumShards)
	}

	srcReadOpts := fs.DataReaderOpenOptions{
		Identifier: fs.FileSetFileIdentifier{
			Namespace:   ident.StringID(namespace),
			Shard:       srcShard,
			BlockStart:  blockStart,
			VolumeIndex: volume,
		},
		FileSetType:      persist.FileSetFlushType,
		StreamingEnabled: true,
	}

	err := srcReader.Open(srcReadOpts)
	if err != nil {
		return fmt.Errorf("unable to open srcReader: %w", err)
	}

	dstEntries := 0
	for i := range dstReaders {
		dstReadOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   ident.StringID(namespace),
				Shard:       mapToDstShard(srcNumShards, i, srcShard),
				BlockStart:  blockStart,
				VolumeIndex: volume + 1,
			},
			FileSetType:      persist.FileSetFlushType,
			StreamingEnabled: true,
		}
		if err := dstReaders[i].Open(dstReadOpts); err != nil {
			return fmt.Errorf("unable to open dstReaders[%d]: %w", i, err)
		}
		dstEntries += dstReaders[i].Entries()
	}

	if srcReader.Entries() != dstEntries {
		return fmt.Errorf("entry count mismatch: src %d != dst %d", srcReader.Entries(), dstEntries)
	}

	for {
		srcEntry, err := srcReader.StreamingReadMetadata()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("src read error: %w", err)
		}

		newShardID := hashFn(srcEntry.ID)
		if newShardID%srcNumShards != srcShard {
			return fmt.Errorf("mismatched shards, %d to %d", srcShard, newShardID)
		}
		dstReader := dstReaders[newShardID/srcNumShards]

		// Using StreamingRead() on destination filesets here because it also verifies data checksums.
		dstEntry, err := dstReader.StreamingRead()
		if err != nil {
			return fmt.Errorf("dst read error: %w", err)
		}

		if !bytes.Equal(srcEntry.ID, dstEntry.ID) {
			return fmt.Errorf("ID mismatch: %s != %s", srcEntry.ID, dstEntry.ID)
		}
		if !bytes.Equal(srcEntry.EncodedTags, dstEntry.EncodedTags) {
			return fmt.Errorf("EncodedTags mismatch: %s != %s", srcEntry.EncodedTags, dstEntry.EncodedTags)
		}
		if srcEntry.DataChecksum != dstEntry.DataChecksum {
			return fmt.Errorf("data checksum mismatch: %d != %d, id=%s",
				srcEntry.DataChecksum, dstEntry.DataChecksum, srcEntry.ID)
		}
	}

	for i := range dstReaders {
		dstReader := dstReaders[i]
		if _, err := dstReader.StreamingReadMetadata(); !errors.Is(err, io.EOF) {
			return fmt.Errorf("expected EOF on split shard %d, but got %w",
				dstReader.Status().Shard, err)
		}
		if err := dstReader.Close(); err != nil {
			return err
		}
	}

	return srcReader.Close()
}

func mapToDstShard(srcNumShards uint32, i int, srcShard uint32) uint32 {
	return srcNumShards*uint32(i) + srcShard
}

func dropDataSuffix(path string) string {
	dataIdx := strings.LastIndex(path, "/data")
	if dataIdx < 0 {
		return path
	}
	return path[:dataIdx]
}
