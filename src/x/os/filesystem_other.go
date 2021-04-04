// +build !linux

package xos

import (
	"fmt"
)

var (
	ErrFileSystemStats = fmt.Errorf("unable to get filesystem stats for non-linux os")
)

func GetFileSystemStats(path string) (*FileSystemStats, error) {
	return nil, ErrFileSystemStats
}
