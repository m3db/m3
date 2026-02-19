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

// GetDirectoryStats returns the total size of all files
// in the directory tree. Note that this will be different
// from the output of the utility du as it only counts
// apparent sizes of files, and not the disk usage of files.
func GetDirectoryStats(path string) (*DirectoryStats, error) {
	ret := &DirectoryStats{}
	err := filepath.Walk(path, func(n string, fInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !fInfo.IsDir() {
			ret.Size += uint64(fInfo.Size())
		}
		return err
	})
	return ret, err
}
