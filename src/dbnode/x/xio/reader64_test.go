package xio

import (
	"encoding/binary"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesReader64(t *testing.T) {
	var (
		data = []byte{4, 5, 6, 7, 8, 9, 1, 2, 3, 0, 10, 11, 12, 13, 14, 15, 16, 17}
		r    = NewBytesReader64(nil)
	)

	for l := 0; l < len(data); l++ {
		testBytesReader64(t, r, data)
	}
}

func testBytesReader64(t *testing.T, r *BytesReader64, data []byte) {
	r.Reset(data)

	var (
		peeked, read []byte
		buf          [8]byte
		word         uint64
		n            byte
		err          error
	)

	for {
		word, n, err = r.Peek64()
		if err != nil {
			break
		}
		binary.BigEndian.PutUint64(buf[:], word)
		peeked = append(peeked, buf[:n]...)

		word, n, err = r.Read64()
		if err != nil {
			break
		}
		binary.BigEndian.PutUint64(buf[:], word)
		read = append(read, buf[:n]...)
	}

	require.Equal(t, io.EOF, err)
	assert.Equal(t, data, peeked)
	assert.Equal(t, data, read)
}
