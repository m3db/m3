package yio

import (
	"io"
)

// NB: headerReader re-adds snappy headers since they are stripped.
type headerReader struct {
	i      int
	err    error
	header []byte
	r      io.Reader
}

var _ io.Reader = (*headerReader)(nil)

func newHeaderReader(header []byte, r io.Reader) io.Reader {
	return &headerReader{
		i:      0,
		header: header,
		r:      r,
	}
}

func (r *headerReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	var n int
	n, r.err = r.read(p)
	return n, r.err
}

func (r *headerReader) read(p []byte) (int, error) {
	if r.i >= len(r.header) {
		// NB: header has been used up, safe to use contained reader.
		return r.r.Read(p)
	}
	need := len(p)
	remainingHeader := len(r.header) - r.i
	if need <= remainingHeader {
		n := copy(p, r.header[r.i:r.i+need])
		r.i = n + r.i
		return n, nil
	}
	n := copy(p, r.header[r.i:])
	r.i = n + r.i
	read, err := r.r.Read(p[remainingHeader:])
	if err != nil {
		return 0, err
	}
	return read + n, nil
}

func (r *headerReader) reset(header []byte, resetReader io.Reader) {
	r.err = nil
	r.i = 0
	r.header = header
	r.r = resetReader
}
