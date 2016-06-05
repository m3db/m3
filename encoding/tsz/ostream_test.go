package tsz

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteBits(t *testing.T) {
	inputs := []struct {
		value         uint64
		numBits       int
		expectedBytes []byte
		expectedPos   int
	}{
		{0x1, 1, []byte{0x80}, 1},
		{0x4, 3, []byte{0xc0}, 4},
		{0xa, 4, []byte{0xca}, 8},
		{0xfe, 8, []byte{0xca, 0xfe}, 8},
		{0xaafe, 7, []byte{0xca, 0xfe, 0xfc}, 7},
		{0x3, 2, []byte{0xca, 0xfe, 0xfd, 0x80}, 1},
		{0x1234567890abcdef, 64, []byte{0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7, 0x80}, 1},
		{0x1, 0, []byte{0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7, 0x80}, 1},
		{0x1, 65, []byte{0xca, 0xfe, 0xfd, 0x89, 0x1a, 0x2b, 0x3c, 0x48, 0x55, 0xe6, 0xf7, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x80}, 1},
	}

	os := newOStream(nil)
	require.True(t, os.empty())
	for _, input := range inputs {
		os.WriteBits(input.value, input.numBits)
		require.Equal(t, input.expectedBytes, os.rawBuffer)
		require.Equal(t, input.expectedPos, os.pos)
	}
	require.False(t, os.empty())
}

func TestWriteBytes(t *testing.T) {
	os := newOStream(nil)
	rawBytes := []byte{0x1, 0x2}
	os.WriteBytes(rawBytes)
	require.Equal(t, rawBytes, os.rawBuffer)
	require.Equal(t, 8, os.pos)
}

func TestResetOStream(t *testing.T) {
	os := newOStream(nil)
	os.WriteByte(0xfe)
	os.Reset()
	require.True(t, os.empty())
	require.Equal(t, 0, os.len())
	require.Equal(t, 0, os.pos)
}
