// Copyright (c) 2016 Uber Technologies, Inc.
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

package fs

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/generated/proto/schema"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/errors"

	"github.com/golang/protobuf/proto"
)

var timeZero time.Time

type fileOpener func(filePath string) (*os.File, error)

func openFiles(opener fileOpener, fds map[string]**os.File) error {
	var firstErr error
	for filePath, fdPtr := range fds {
		fd, err := opener(filePath)
		if err != nil {
			firstErr = err
			break
		}
		*fdPtr = fd
	}

	if firstErr == nil {
		return nil
	}

	// If we have encountered an error when opening the files,
	// close the ones that have been opened.
	for _, fdPtr := range fds {
		if *fdPtr != nil {
			(*fdPtr).Close()
		}
	}

	return firstErr
}

// TODO(xichen): move closeAll to m3x/close.
func closeAll(closers ...xclose.Closer) error {
	multiErr := xerrors.NewMultiError()
	for _, closer := range closers {
		if err := closer.Close(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

// byTimeAscending sorts files by their block start times in ascending order.
// If the files do not have block start times in their names, the result is undefined.
type byTimeAscending []string

func (a byTimeAscending) Len() int      { return len(a) }
func (a byTimeAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byTimeAscending) Less(i, j int) bool {
	ti, _ := TimeFromFileName(a[i])
	tj, _ := TimeFromFileName(a[j])
	return ti.Before(tj)
}

// TimeFromFileName extracts the block start time from file name.
func TimeFromFileName(fname string) (time.Time, error) {
	components := strings.Split(filepath.Base(fname), separator)
	if len(components) < 2 {
		return timeZero, fmt.Errorf("unexpected file name %s", fname)
	}
	latestTimeInNano, err := strconv.ParseInt(components[0], 10, 64)
	if err != nil {
		return timeZero, err
	}
	return time.Unix(0, latestTimeInNano), nil
}

type infoFileFn func(fname string, infoData []byte)

// ForEachInfoFile iterates over each valid info file and applies the function passed in.
func ForEachInfoFile(filePathPrefix string, shard uint32, fn infoFileFn) {
	shardDir := shardDirPath(filePathPrefix, shard)
	matched, err := filepath.Glob(path.Join(shardDir, infoFilePattern))
	if err != nil {
		return
	}

	sort.Sort(byTimeAscending(matched))
	digestBuf := digest.NewBuffer()
	for i := range matched {
		t, err := TimeFromFileName(matched[i])
		if err != nil {
			continue
		}
		checkpointFilePath := filepathFromTime(shardDir, t, checkpointFileSuffix)
		if !fileExists(checkpointFilePath) {
			continue
		}
		checkpointFd, err := os.Open(checkpointFilePath)
		if err != nil {
			continue
		}
		// Read digest of digests from the checkpoint file
		expectedDigestOfDigest, err := digestBuf.ReadDigestFromFile(checkpointFd)
		checkpointFd.Close()
		if err != nil {
			continue
		}
		// Read and validate the digest file
		digestData, err := readAndValidate(shardDir, t, digestFileSuffix, expectedDigestOfDigest)
		if err != nil {
			continue
		}
		// Read and validate the info file
		expectedInfoDigest := digest.ToBuffer(digestData).ReadDigest()
		infoData, err := readAndValidate(shardDir, t, infoFileSuffix, expectedInfoDigest)
		if err != nil {
			continue
		}
		fn(matched[i], infoData)
	}
}

// ReadInfoFiles reads all the valid info entries.
func ReadInfoFiles(filePathPrefix string, shard uint32) []*schema.IndexInfo {
	var indexEntries []*schema.IndexInfo
	ForEachInfoFile(
		filePathPrefix,
		shard,
		func(_ string, data []byte) {
			info, err := readInfo(data)
			if err != nil {
				return
			}
			indexEntries = append(indexEntries, info)
		},
	)
	return indexEntries
}

// readInfo reads the data stored in the info file and returns an index entry.
func readInfo(data []byte) (*schema.IndexInfo, error) {
	info := &schema.IndexInfo{}
	if err := proto.Unmarshal(data, info); err != nil {
		return nil, err
	}
	return info, nil
}

func readAndValidate(prefix string, t time.Time, suffix string, expectedDigest uint32) ([]byte, error) {
	filePath := filepathFromTime(prefix, t, suffix)
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	fwd := digest.NewFdWithDigestReader()
	fwd.Reset(fd)
	return fwd.ReadAllAndValidate(expectedDigest)
}

// FileExistsAt determines whether a data file exists for the given shard and block start time.
func FileExistsAt(prefix string, shard uint32, blockStart time.Time) bool {
	shardDir := shardDirPath(prefix, shard)
	checkpointFile := filepathFromTime(shardDir, blockStart, checkpointFileSuffix)
	return fileExists(checkpointFile)
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

// shardDirPath returns the path to a given shard.
func shardDirPath(prefix string, shard uint32) string {
	return path.Join(prefix, strconv.Itoa(int(shard)))
}

func filepathFromTime(prefix string, t time.Time, suffix string) string {
	return path.Join(prefix, fmt.Sprintf("%d%s%s", t.UnixNano(), separator, suffix))
}
