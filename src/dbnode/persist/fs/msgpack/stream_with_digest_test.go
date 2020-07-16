package msgpack

import (
	"bufio"
	"bytes"
	"hash/adler32"
	"testing"

	"github.com/stretchr/testify/require"
)

const srcString = "foo bar baz qux quux corge grault"

func TestDecoderStreamWithDigestRead(t *testing.T) {
	stream := newTestDecoderStream()

	// Read srcString in chunkLen size chunks
	chunkLen := 3
	buf := make([]byte, len(srcString))
	for start := 0; start < len(srcString); start = start + chunkLen {
		end := start + chunkLen
		if end > len(srcString) {
			end = len(srcString)
		}

		n, err := stream.Read(buf[start:end])
		require.NoError(t, err)
		require.Equal(t, chunkLen, n)
		require.Equal(t, adler32.Checksum(buf[:end]), stream.Digest().Sum32())
	}
}

func TestDecoderStreamWithDigestReadByte(t *testing.T) {
	stream := newTestDecoderStream()

	buf := make([]byte, len(srcString))
	for i := 1; i < len(srcString); i++ {
		n, err := stream.Read(buf[i-1 : i])
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, adler32.Checksum(buf[:i]), stream.Digest().Sum32())
	}
}

func TestDecoderStreamWithDigestUnreadByte(t *testing.T) {
	stream := decoderStreamWithDigest{
		reader: bufio.NewReader(bytes.NewReader([]byte(srcString))),
		digest: adler32.New(),
	}

	b, err := stream.ReadByte()
	require.NoError(t, err)
	require.Equal(t, srcString[0], b)
	require.False(t, stream.unreadByte)

	err = stream.UnreadByte()
	require.NoError(t, err)
	require.True(t, stream.unreadByte)
}

func TestDecoderStreamWithDigestReset(t *testing.T) {
	stream := newTestDecoderStream()

	b, err := stream.ReadByte()
	require.NoError(t, err)
	require.Equal(t, srcString[0], b)

	b, err = stream.ReadByte()
	require.NoError(t, err)
	require.Equal(t, srcString[1], b)

	stream.Reset(bufio.NewReader(bytes.NewReader([]byte(srcString))))

	b, err = stream.ReadByte()
	require.NoError(t, err)
	require.Equal(t, srcString[0], b)
}

func TestDecoderStreamWithDigestValidate(t *testing.T) {
	stream := newTestDecoderStream()
	buf := make([]byte, 5)

	n, err := stream.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 5, n)

	require.NoError(t, stream.Validate(adler32.Checksum(buf)))
	require.Error(t, stream.Validate(adler32.Checksum([]byte("asdf"))))
}

func TestDecoderStreamWithDigestCapture(t *testing.T) {
	stream := newTestDecoderStream()

	require.NoError(t, stream.Validate(1))

	bytes := []byte("manual capture")
	require.NoError(t, stream.Capture(bytes))

	require.Equal(t, adler32.Checksum(bytes), stream.Digest().Sum32())
}

func TestDecoderStreamWithDigestReadUnreadRead(t *testing.T) {
	stream := newTestDecoderStream()

	buf := make([]byte, len(srcString))
	end := 0

	b1, err := stream.ReadByte()
	require.NoError(t, err)
	buf[0] = b1
	end++
	require.Equal(t, adler32.Checksum(buf[:end]), stream.Digest().Sum32())

	err = stream.UnreadByte()
	end--
	require.NoError(t, err)

	b2, err := stream.ReadByte()
	require.NoError(t, err)
	end++
	require.Equal(t, b1, b2)
	require.Equal(t, adler32.Checksum(buf[:end]), stream.Digest().Sum32())

	n, err := stream.Read(buf[end : end+4])
	require.NoError(t, err)
	require.Equal(t, 4, n)
	end += n
	require.Equal(t, adler32.Checksum(buf[:end]), stream.Digest().Sum32())

	err = stream.UnreadByte()
	end--
	require.NoError(t, err)

	n, err = stream.Read(buf[end : end+4])
	require.NoError(t, err)
	require.Equal(t, 4, n)
	end += n
	require.Equal(t, adler32.Checksum(buf[:end]), stream.Digest().Sum32())
}

func newTestDecoderStream() DecoderStreamWithDigest {
	return newDecoderStreamWithDigest(bufio.NewReader(bytes.NewReader([]byte(srcString))))
}
