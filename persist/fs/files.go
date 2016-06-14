package fs

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	schema "code.uber.internal/infra/memtsdb/persist/fs/proto"

	"github.com/golang/protobuf/proto"
)

var timeZero time.Time

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

// InfoFiles returns all the completed info files in the given shard
// directory, sorted by their corresponding block start times.
func InfoFiles(shardDir string) ([]string, error) {
	matched, err := filepath.Glob(path.Join(shardDir, infoFilePattern))
	if err != nil {
		return nil, err
	}
	j := 0
	for i := range matched {
		t, err := TimeFromFileName(matched[i])
		if err != nil {
			return nil, err
		}
		checkpointFile := filepathFromTime(shardDir, t, checkpointFileSuffix)
		if fileExists(checkpointFile) {
			matched[j] = matched[i]
			j++
		}
	}
	matched = matched[:j]
	sort.Sort(byTimeAscending(matched))
	return matched, nil
}

// FileExistsAt determines whether a data file exists for the given shard and block start time.
func FileExistsAt(prefix string, shard uint32, blockStart time.Time) bool {
	shardDir := ShardDirPath(prefix, shard)
	checkpointFile := filepathFromTime(shardDir, blockStart, checkpointFileSuffix)
	return fileExists(checkpointFile)
}

// ReadInfo reads the info file.
func ReadInfo(infoFd *os.File) (*schema.IndexInfo, error) {
	data, err := ioutil.ReadAll(infoFd)
	if err != nil {
		return nil, err
	}

	info := &schema.IndexInfo{}
	if err := proto.Unmarshal(data, info); err != nil {
		return nil, err
	}
	return info, nil
}

// ShardDirPath returns the path to a given shard.
func ShardDirPath(prefix string, shard uint32) string {
	return path.Join(prefix, strconv.Itoa(int(shard)))
}

func fileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

func filepathFromTime(prefix string, t time.Time, suffix string) string {
	return path.Join(prefix, fmt.Sprintf("%d%s%s", t.UnixNano(), separator, suffix))
}
