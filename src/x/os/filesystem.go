package xos

// Stats on the filesystem.
type FileSystemStats struct {
	Total uint64 // Total space in the filesystem in bytes
	Avail uint64 // Available space in the filesystem in bytes
}
