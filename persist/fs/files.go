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
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3db/generated/proto/schema"

	"github.com/golang/protobuf/proto"
)

var (
	timeZero time.Time

	// errCheckpointFileNotFound returned when the checkpoint file doesn't exist
	errCheckpointFileNotFound = errors.New("checkpoint file does not exist")
)

type fileOpener func(filePath string) (*os.File, error)

func openFiles(opener fileOpener, fds map[string]**os.File) error {
	for filePath, fdPtr := range fds {
		fd, err := opener(filePath)
		if err != nil {
			return err
		}
		*fdPtr = fd
	}
	return nil
}

func closeFiles(fds ...*os.File) error {
	for _, fd := range fds {
		if err := fd.Close(); err != nil {
			return err
		}
	}
	return nil
}

func validFiles(fds ...*os.File) []*os.File {
	var vf []*os.File
	for _, fd := range fds {
		if fd == nil {
			continue
		}
		vf = append(vf, fd)
	}
	return vf
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

// readCheckpointFile reads the checkpoint file and returns the digest stored
// in the checkpoint file. Note that buf has digestSize bytes to avoid gc and
// repeated memory allocation.
func readCheckpointFile(shardDir string, blockStart time.Time, buf []byte) (uint32, error) {
	checkpointFilePath := filepathFromTime(shardDir, blockStart, checkpointFileSuffix)
	if !fileExists(checkpointFilePath) {
		return 0, errCheckpointFileNotFound
	}
	checkpointFd, err := os.Open(checkpointFilePath)
	if err != nil {
		return 0, err
	}
	defer checkpointFd.Close()
	return readDigestFromFile(checkpointFd, buf)
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
	buf := make([]byte, digestLen)
	for i := range matched {
		t, err := TimeFromFileName(matched[i])
		if err != nil {
			continue
		}
		expectedDigestOfDigest, err := readCheckpointFile(shardDir, t, buf)
		if err != nil {
			continue
		}
		// Read and validate the digest file
		digestData, err := readAndValidate(shardDir, t, digestFileSuffix, expectedDigestOfDigest)
		if err != nil {
			continue
		}
		// Read and validate the info file
		expectedInfoDigest := readDigest(digestData[:digestLen])
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

// FileExistsAt determines whether a data file exists for the given shard and block start time.
func FileExistsAt(prefix string, shard uint32, blockStart time.Time) bool {
	shardDir := shardDirPath(prefix, shard)
	checkpointFile := filepathFromTime(shardDir, blockStart, checkpointFileSuffix)
	return fileExists(checkpointFile)
}

// shardDirPath returns the path to a given shard.
func shardDirPath(prefix string, shard uint32) string {
	return path.Join(prefix, strconv.Itoa(int(shard)))
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

func readAndValidate(prefix string, t time.Time, suffix string, expectedDigest uint32) ([]byte, error) {
	filePath := filepathFromTime(prefix, t, suffix)
	fd, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	fwd := newFdWithDigest()
	fwd.fd = fd
	return fwd.readAllAndValidate(expectedDigest)
}

func filepathFromTime(prefix string, t time.Time, suffix string) string {
	return path.Join(prefix, fmt.Sprintf("%d%s%s", t.UnixNano(), separator, suffix))
}
