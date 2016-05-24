package encoding

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSliceOfSliceReaderNilData(t *testing.T) {
	r := NewSliceOfSliceReader(nil)
	var buf []byte
	n, err := r.Read(buf)
	require.Equal(t, 0, n)
	require.NoError(t, err)

	buf = make([]byte, 1)
	n, err = r.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)
}

func TestSliceOfSliceReaderOneSlice(t *testing.T) {
	bytes := [][]byte{
		{0x1, 0x2, 0x3},
	}
	r := NewSliceOfSliceReader(bytes)
	buf := make([]byte, 1)
	for i := 0; i < 3; i++ {
		n, err := r.Read(buf)
		require.Equal(t, 1, n)
		require.Equal(t, bytes[0][i:i+1], buf[:n])
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		n, err := r.Read(buf)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	}

	r = NewSliceOfSliceReader(bytes)
	buf = make([]byte, 2)
	for i := 0; i < 2; i++ {
		n, err := r.Read(buf)
		if i == 0 {
			require.Equal(t, 2, n)
			require.Equal(t, bytes[0][0:2], buf[:n])
		} else {
			require.Equal(t, 1, n)
			require.Equal(t, bytes[0][2:], buf[:n])
		}
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		n, err := r.Read(buf)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	}
}

func TestSliceOfSliceReaderMultiSlice(t *testing.T) {
	bytes := [][]byte{
		{0x1, 0x2, 0x3},
		{0x4, 0x5},
		{0x6, 0x7, 0x8, 0x9},
	}
	r := NewSliceOfSliceReader(bytes)
	buf := make([]byte, 3)
	expectedResults := []struct {
		n    int
		data []byte
	}{
		{3, []byte{0x1, 0x2, 0x3}},
		{3, []byte{0x4, 0x5, 0x6}},
		{3, []byte{0x7, 0x8, 0x9}},
	}
	for i := 0; i < 3; i++ {
		n, err := r.Read(buf)
		require.Equal(t, expectedResults[i].n, n)
		require.Equal(t, expectedResults[i].data, buf[:n])
		require.NoError(t, err)
	}
	for i := 0; i < 2; i++ {
		n, err := r.Read(buf)
		require.Equal(t, 0, n)
		require.Equal(t, io.EOF, err)
	}
}
