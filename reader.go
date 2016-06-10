package memtsdb

import (
	"io"
)

// ReaderSliceReader implements the io reader interface backed by a slice of readers.
type ReaderSliceReader interface {
	io.Reader

	Readers() []io.Reader
}

// SegmentReader implements the io reader interface backed by a segment.
type SegmentReader interface {
	io.Reader

	// Segment gets the segment read by this reader.
	Segment() Segment

	// Reset resets the reader to read a new segment.
	Reset(segment Segment)

	// Close closes the reader and if pooled will return to the pool.
	Close()
}

// Segment represents a binary blob consisting of two byte slices.
type Segment struct {
	// Head is the head of the segment.
	Head []byte

	// Tail is the tail of the segment.
	Tail []byte
}
