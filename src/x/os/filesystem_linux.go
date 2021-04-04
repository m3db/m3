package xos

import (
	"golang.org/x/sys/unix"
)

func GetFileSystemStats(path string) (*FileSystemStats, error) {
	stats := &unix.Statfs_t{}
	if err := unix.Statfs(path, stats); err != nil {
		return nil, err
	}
	ret := &FileSystemStats{}
	ret.Avail = stats.Bavail * uint64(stats.Bsize)
	ret.Total = stats.Blocks * uint64(stats.Bsize)
	return ret, nil
}
