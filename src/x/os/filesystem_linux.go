package xos

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// GetFileSystemStats returns stats for filesystem rooted
// at rootDir. Stats includes available and total space.
func GetFileSystemStats(rootDir string) (*FileSystemStats, error) {
	stats := &unix.Statfs_t{}
	if err := unix.Statfs(rootDir, stats); err != nil {
		return nil, err
	}
	ret := &FileSystemStats{}
	ret.Avail = stats.Bavail * uint64(stats.Bsize)
	ret.Total = stats.Blocks * uint64(stats.Bsize)
	return ret, nil
}

// GetDirectoryStats returns stats for directory.
// Stats includes total size of all files (not subdirectories)
// in the directory.
func GetDirectoryStats(path string) (*DirectoryStats, error) {
	ret := &DirectoryStats{}
	err := filepath.Walk(path, func(n string, fInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		ret.Size += uint64(fInfo.Size())
		return err
	})
	return ret, err
}
