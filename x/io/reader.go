package io

import (
	"errors"
	"io"

	"code.uber.internal/infra/memtsdb"
)

var (
	errUnexpectedReaderType = errors.New("unexpected reader type")
)

type sliceOfSliceReader struct {
	s  [][]byte // raw data
	si int      // current slice index
	bi int      // current byte index
}

// NewSliceOfSliceReader creates a new io reader that wraps a slice of slice inside itself.
func NewSliceOfSliceReader(rawBytes [][]byte) io.Reader {
	return &sliceOfSliceReader{s: rawBytes}
}

func (r *sliceOfSliceReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.si >= len(r.s) {
		return 0, io.EOF
	}
	n := 0
	for r.si < len(r.s) && n < len(b) {
		nRead := copy(b[n:], r.s[r.si][r.bi:])
		n += nRead
		r.bi += nRead
		if r.bi >= len(r.s[r.si]) {
			r.si++
			r.bi = 0
		}
	}
	return n, nil
}

type readerSliceReader struct {
	s  []io.Reader // reader list
	si int         // slice index
}

// NewReaderSliceReader creates a new ReaderSliceReader instance.
func NewReaderSliceReader(r []io.Reader) memtsdb.ReaderSliceReader {
	return &readerSliceReader{s: r}
}

func (r *readerSliceReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	if r.si >= len(r.s) {
		return 0, io.EOF
	}
	n := 0
	for r.si < len(r.s) && n < len(b) {
		if r.s[r.si] == nil {
			r.si++
			continue
		}
		nRead, _ := r.s[r.si].Read(b[n:])
		n += nRead
		if n < len(b) {
			r.si++
		}
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (r *readerSliceReader) Readers() []io.Reader {
	return r.s
}

type segmentReader struct {
	segment memtsdb.Segment
	si      int
	pool    memtsdb.SegmentReaderPool
}

// NewSegmentReader creates a new segment reader.
func NewSegmentReader(segment memtsdb.Segment) memtsdb.SegmentReader {
	return &segmentReader{segment: segment}
}

// NewPooledSegmentReader creates a new pooled segment reader.
func NewPooledSegmentReader(segment memtsdb.Segment, pool memtsdb.SegmentReaderPool) memtsdb.SegmentReader {
	return &segmentReader{segment: segment, pool: pool}
}

func (sr *segmentReader) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	head, tail := sr.segment.Head, sr.segment.Tail
	nh, nt := len(head), len(tail)
	if sr.si >= nh+nt {
		return 0, io.EOF
	}
	n := 0
	if sr.si < nh {
		nRead := copy(b, head[sr.si:])
		sr.si += nRead
		n += nRead
		if n == len(b) {
			return n, nil
		}
	}
	if sr.si < nh+nt {
		nRead := copy(b[n:], tail[sr.si-nh:])
		sr.si += nRead
		n += nRead
	}
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

func (sr *segmentReader) Segment() memtsdb.Segment {
	return sr.segment
}

func (sr *segmentReader) Reset(segment memtsdb.Segment) {
	sr.segment = segment
	sr.si = 0
}

func (sr *segmentReader) Close() {
	if sr.pool != nil {
		sr.pool.Put(sr)
	}
}

// GetSegmentReaders returns the segment readers contained in an io reader.
func GetSegmentReaders(r io.Reader) ([]memtsdb.SegmentReader, error) {
	if r == nil {
		return nil, nil
	}
	sr, ok := r.(memtsdb.ReaderSliceReader)
	if !ok {
		return nil, errUnexpectedReaderType
	}
	readers := sr.Readers()
	s := make([]memtsdb.SegmentReader, 0, len(readers))
	for i := 0; i < len(readers); i++ {
		if readers[i] == nil {
			continue
		}
		sgr, ok := readers[i].(memtsdb.SegmentReader)
		if !ok {
			return nil, errUnexpectedReaderType
		}
		s = append(s, sgr)
	}
	return s, nil
}
