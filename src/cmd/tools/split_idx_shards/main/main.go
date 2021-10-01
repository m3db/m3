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
	iofs "io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/getopt"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/generated/proto/index"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	xos "github.com/m3db/m3/src/x/os"
	xtime "github.com/m3db/m3/src/x/time"
)

var checkpointPattern = regexp.MustCompile(`/index/data/(\w+)/fileset-([0-9]+)-([0-9]+)-checkpoint.db$`)

func main() {
	var (
		optPath       = getopt.StringLong("path", 'p', "", "Index path [e.g. /temp/lib/m3db/index]")
		optBlockUntil = getopt.Int64Long("block-until", 'b', 0, "Block Until Time, exclusive [in nsec]")
		optSrcShards  = getopt.Uint32Long("src-shards", 'h', 0, "Original (source) number of shards")
		optFactor     = getopt.IntLong("factor", 'f', 0, "Integer factor to increase the number of shards by")
	)
	getopt.Parse()

	rawLogger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("unable to create logger: %+v", err)
	}
	logger := rawLogger.Sugar()

	if *optPath == "" ||
		*optBlockUntil <= 0 ||
		*optSrcShards == 0 ||
		*optFactor <= 0 {
		getopt.Usage()
		os.Exit(1)
	}

	var (
		filesetLocation = dropIndexSuffix(*optPath)
		fsOpts          = fs.NewOptions().SetFilePathPrefix(filesetLocation)
	)

	start := time.Now()

	if err := filepath.WalkDir(*optPath, func(path string, d iofs.DirEntry, err error) error {
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
			blockStart, err1 = strconv.Atoi(pathParts[2])
			volume, err2     = strconv.Atoi(pathParts[3])
		)
		if err = xerrors.FirstError(err1, err2); err != nil {
			return err
		}

		if blockStart >= int(*optBlockUntil) {
			fmt.Println(" - skip (too recent)") // nolint: forbidigo
			return nil
		}

		namespaceDir := fs.NamespaceIndexDataDirPath(filesetLocation, ident.StringID(namespace))

		if err = updateIndexInfoFile(
			namespaceDir, xtime.UnixNano(blockStart), volume, *optSrcShards, *optFactor, fsOpts); err != nil {
			if strings.Contains(err.Error(), "no such file or directory") {
				fmt.Println(" - skip (incomplete fileset)") // nolint: forbidigo
				return nil
			}
			return err
		}

		return err
	}); err != nil {
		logger.Fatalf("unable to walk the source dir: %+v", err)
	}

	runTime := time.Since(start)
	fmt.Printf("Running time: %s\n", runTime) // nolint: forbidigo
}

func updateIndexInfoFile(
	namespaceDir string,
	blockStart xtime.UnixNano,
	volume int,
	srcNumShards uint32,
	factor int,
	fsOpts fs.Options,
) error {
	var (
		infoFilePath       = fs.FilesetPathFromTimeAndIndex(namespaceDir, blockStart, volume, fs.InfoFileSuffix)
		digestFilePath     = fs.FilesetPathFromTimeAndIndex(namespaceDir, blockStart, volume, fs.DigestFileSuffix)
		checkpointFilePath = fs.FilesetPathFromTimeAndIndex(namespaceDir, blockStart, volume, fs.CheckpointFileSuffix)

		info    = index.IndexVolumeInfo{}
		digests = index.IndexDigests{}
	)

	digestsData, err := ioutil.ReadFile(digestFilePath) // nolint: gosec
	if err != nil {
		return err
	}
	if err = digests.Unmarshal(digestsData); err != nil {
		return err
	}

	infoData, err := ioutil.ReadFile(infoFilePath) // nolint: gosec
	if err != nil {
		return err
	}
	if err = info.Unmarshal(infoData); err != nil {
		return err
	}

	var newShards []uint32
	for _, srcShard := range info.Shards {
		if srcShard >= srcNumShards {
			return fmt.Errorf("unexpected source shard ID %d (must be under %d)", srcShard, srcNumShards)
		}
		for i := 0; i < factor; i++ {
			dstShard := mapToDstShard(srcNumShards, i, srcShard)
			newShards = append(newShards, dstShard)
		}
	}
	info.Shards = newShards

	newInfoData, err := info.Marshal()
	if err != nil {
		return err
	}

	if err = xos.WriteFileSync(infoFilePath, newInfoData, fsOpts.NewFileMode()); err != nil {
		return err
	}

	digests.InfoDigest = digest.Checksum(newInfoData)
	newDigestsData, err := digests.Marshal()
	if err != nil {
		return err
	}

	if err = ioutil.WriteFile(digestFilePath, newDigestsData, fsOpts.NewFileMode()); err != nil {
		return err
	}

	digestBuffer := digest.NewBuffer()
	digestBuffer.WriteDigest(digest.Checksum(newDigestsData))

	return ioutil.WriteFile(checkpointFilePath, digestBuffer, fsOpts.NewFileMode())
}

func mapToDstShard(srcNumShards uint32, i int, srcShard uint32) uint32 {
	return srcNumShards*uint32(i) + srcShard
}

func dropIndexSuffix(path string) string {
	idx := strings.LastIndex(path, "/index")
	if idx < 0 {
		return path
	}
	return path[:idx]
}
