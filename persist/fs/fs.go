package fs

import (
	"encoding/binary"
	"io"
	"time"

	xtime "code.uber.internal/infra/memtsdb/x/time"
)

// TODO(xichen): move interfaces to top-level

const (
	infoFileSuffix       = "info.db"
	indexFileSuffix      = "index.db"
	dataFileSuffix       = "data.db"
	checkpointFileSuffix = "checkpoint.db"

	separator       = "-"
	infoFilePattern = "[0-9]*" + separator + infoFileSuffix

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

	// Open opens the files for writing data to the given shard.
	Open(shard uint32, start time.Time) error

	// Write will write the key and data pair and returns an error on a write error
	Write(key string, data []byte) error

	// WriteAll will write the key and all byte slices and returns an error on a write error.
	WriteAll(key string, data [][]byte) error
}

// Reader provides an unsynchronized reader for a TSDB file set
type Reader interface {
	io.Closer

	// Open opens the files for the given shard and version for reading.
	Open(shard uint32, start time.Time) error

	// Read returns the next key and data pair or error, will return io.EOF at end of volume
	Read() (key string, data []byte, err error)

	// Range returns the time range associated with data in the volume.
	Range() xtime.Range

	// Entries returns the count of entries in the volume
	Entries() int

	// EntriesRead returns the position read into the volume
	EntriesRead() int
}

// NewWriterFn creates a new writer.
type NewWriterFn func(blockSize time.Duration, filePathPrefix string) Writer

// NewReaderFn creates a new reader.
type NewReaderFn func(filePathPrefix string) Reader
