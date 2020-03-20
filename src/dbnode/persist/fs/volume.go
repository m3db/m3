package fs

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/m3db/m3/src/x/ident"
)

// GetVolumeIndices returns available volume indices for specified options.
func GetVolumeIndices(
	filePathPrefix string,
	namespace ident.ID,
	shard uint32,
	blockStart time.Time,
) ([]int, error) {
	shardDir := ShardDataDirPath(filePathPrefix, namespace, shard)
	// Get volume indices based on info files.
	r := regexp.MustCompile(fmt.Sprintf(
		`%s/%s%s%d%s(?P<VolumeIndex>\d+)%s%s%s`,
		shardDir,
		filesetFilePrefix,
		separator,
		blockStart.UnixNano(),
		separator,
		separator,
		infoFileSuffix,
		fileSuffix,
	))

	var volumeIndices []int
	if err := filepath.Walk(shardDir, func(path string, _ os.FileInfo, err error) error {
		match := r.FindStringSubmatch(path)
		// When we match, the second result is the capture group `VolumeIndex`.
		if len(match) == 2 {
			idx, err := strconv.ParseInt(match[1], 0, 64)
			if err != nil {
				return err
			}
			volumeIndices = append(volumeIndices, int(idx))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return volumeIndices, nil
}
