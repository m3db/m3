package tsz

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadBits(t *testing.T) {
	byteStream := []byte{
		0xa9, 0xfe, 0xfe, 0xdf, 0x9b, 0x57, 0x21, 0xf1, 0xac,
		0x68, 0x24, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
	}
	is := newIStream(bytes.NewReader(byteStream))
	numBits := []int{1, 3, 4, 8, 7, 2, 64, 64}
	var res []uint64
	for _, v := range numBits {
		read, err := is.readBits(v)
		require.NoError(t, err)
		res = append(res, read)
	}
	expected := []uint64{0x1, 0x4, 0xa, 0xfe, 0x7e, 0x3, 0x1234567890abcdef, 0x1}
	require.Equal(t, expected, res)
	require.NoError(t, is.err)

	_, err := is.readBits(8)
	require.Error(t, err)
	require.Error(t, is.err)
}

func TestResetIStream(t *testing.T) {
	is := newIStream(bytes.NewReader(nil))
	is.readBits(1)
	require.Error(t, is.err)
	is.reset(bytes.NewReader(nil))
	require.NoError(t, is.err)
	require.Equal(t, byte(0), is.current)
	require.Equal(t, 0, is.remaining)
}
