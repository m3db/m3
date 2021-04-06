// +build !linux

package xos

import (
	"fmt"
)

var (
	ErrFileSystemStats = fmt.Errorf("unable to get filesystem stats for non-linux os")
	ErrDirectoryStats  = fmt.Errorf("unable to get directory stats for non-linux os")
)

func GetFileSystemStats(path string) (*FileSystemStats, error) {
	return nil, ErrFileSystemStats
}

func GetDirectoryStats(path string) (*DirectoryStats, error) {
	return nil, ErrDirectoryStats
}
