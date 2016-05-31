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

	schema "code.uber.internal/infra/memtsdb/persist/fs/proto"

	"github.com/golang/protobuf/proto"
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

// byVersionAscending sorts files by their version number in ascending order.
// If the files do not have version numbers in their names, the result is undefined.
type byVersionAscending []string

func (a byVersionAscending) Len() int      { return len(a) }
func (a byVersionAscending) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byVersionAscending) Less(i, j int) bool {
	vi, _ := VersionFromName(a[i])
	vj, _ := VersionFromName(a[j])
	return vi < vj
}

// InfoFiles returns all the info files in the given shard directory,
// sorted by versions.
func InfoFiles(shardDir string) ([]string, error) {
	matched, err := filepath.Glob(path.Join(shardDir, infoFilePattern))
	if err != nil {
		return nil, err
	}
	sort.Sort(byVersionAscending(matched))
	return matched, nil
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

// VersionFromName extracts the version number from file name.
func VersionFromName(fname string) (int, error) {
	components := strings.Split(filepath.Base(fname), separator)
	if len(components) == 0 {
		return -1, fmt.Errorf("unexpected file name %s", fname)
	}
	latestVersion, err := strconv.Atoi(components[0])
	if err != nil {
		return -1, err
	}
	return latestVersion, nil
}

// ShardDirPath returns the path to a given shard.
func ShardDirPath(prefix string, shard uint32) string {
	return path.Join(prefix, strconv.Itoa(int(shard)))
}

// nextVersion returns the next available version for given shard directory.
func nextVersion(shardDir string) (int, error) {
	infoFiles, err := InfoFiles(shardDir)
	if err != nil {
		return -1, err
	}
	if len(infoFiles) == 0 {
		return 0, nil
	}
	latestVersion, err := VersionFromName(infoFiles[len(infoFiles)-1])
	if err != nil {
		return -1, err
	}
	return latestVersion + 1, nil
}

func filepathFromVersion(prefix string, version int, suffix string) string {
	return path.Join(prefix, fmt.Sprintf("%d%s%s", version, separator, suffix))
}
