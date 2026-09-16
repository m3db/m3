// +build !linux

package xos

import (
	"fmt"
)

var (
	// ErrFileSystemStats filesystem stats not supported
	ErrFileSystemStats = fmt.Errorf("unable to get filesystem stats for non-linux os")
	// ErrDirectoryStats directory stats not supported
	ErrDirectoryStats = fmt.Errorf("unable to get directory stats for non-linux os")
)

// GetFileSystemStats returns stats for filesystem
func GetFileSystemStats(path string) (*FileSystemStats, error) {
	return nil, ErrFileSystemStats
}

// GetDirectoryStats returns stats for directory
func GetDirectoryStats(path string) (*DirectoryStats, error) {
	return nil, ErrDirectoryStats
}
