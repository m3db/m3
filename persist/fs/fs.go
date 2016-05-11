package fs

import (
	"encoding/binary"
	"io"
)

const (
	infoFileSuffix  = "info.db"
	indexFileSuffix = "index.db"
	dataFileSuffix  = "data.db"

	// Index ID is int64
	idxLen = 8
)

var (
	// Use an easy marker for out of band analyzing the raw data files
	marker    = []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1}
	markerLen = len(marker)

	// Endianness is little endian
	endianness = binary.LittleEndian
)

// Writer provides an unsynchronized writer for a TSDB file set
type Writer interface {
	io.Closer

	// Write will write the key and data pair and returns an error on a write error
	Write(key string, data []byte) error
}

// Reader provides an unsynchronized reader for a TSDB file set
type Reader interface {
	io.Closer

	// Read returns the next key and data pair or error, will return io.EOF at end of volume
	Read() (key string, data []byte, err error)

	// Entries returns the count of entries in the volume
	Entries() int

	// EntriesRead returns the position read into the volume
	EntriesRead() int
}
