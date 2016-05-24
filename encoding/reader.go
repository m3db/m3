package encoding

import "io"

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
