// Package xos - filesystem metrics
package xos

// FileSystemStats keeps available and total space
type FileSystemStats struct {
	Total uint64 // Total space in the filesystem in bytes
	Avail uint64 // Available space in the filesystem in bytes
}

// DirectoryStats keeps size
type DirectoryStats struct {
	Size uint64 // Size of the directory
}
