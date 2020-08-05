// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
		require.Equal(t, adler32.Checksum(buf[:end]), stream.digest().Sum32())
	}
}

func TestDecoderStreamWithDigestReadByte(t *testing.T) {
	stream := newTestDecoderStream()

	buf := make([]byte, len(srcString))
	for i := 1; i < len(srcString); i++ {
		n, err := stream.Read(buf[i-1 : i])
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, adler32.Checksum(buf[:i]), stream.digest().Sum32())
	}
}

func TestDecoderStreamWithDigestUnreadByte(t *testing.T) {
	stream := decoderStreamWithDigest{
		reader:       bufio.NewReader(bytes.NewReader([]byte(srcString))),
		readerDigest: adler32.New(),
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

	stream.reset(bufio.NewReader(bytes.NewReader([]byte(srcString))))

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

	require.NoError(t, stream.validate(adler32.Checksum(buf)))
	require.Error(t, stream.validate(adler32.Checksum([]byte("asdf"))))
}

func TestDecoderStreamWithDigestCapture(t *testing.T) {
	stream := newTestDecoderStream()

	require.NoError(t, stream.validate(1))

	bytes := []byte("manual capture")
	require.NoError(t, stream.capture(bytes))

	require.Equal(t, adler32.Checksum(bytes), stream.digest().Sum32())
}

func TestDecoderStreamWithDigestReadUnreadRead(t *testing.T) {
	stream := newTestDecoderStream()

	buf := make([]byte, len(srcString))
	end := 0

	b1, err := stream.ReadByte()
	require.NoError(t, err)
	buf[0] = b1
	end++
	require.Equal(t, adler32.Checksum(buf[:end]), stream.digest().Sum32())

	err = stream.UnreadByte()
	end--
	require.NoError(t, err)

	b2, err := stream.ReadByte()
	require.NoError(t, err)
	end++
	require.Equal(t, b1, b2)
	require.Equal(t, adler32.Checksum(buf[:end]), stream.digest().Sum32())

	n, err := stream.Read(buf[end : end+4])
	require.NoError(t, err)
	require.Equal(t, 4, n)
	end += n
	require.Equal(t, adler32.Checksum(buf[:end]), stream.digest().Sum32())

	err = stream.UnreadByte()
	end--
	require.NoError(t, err)

	n, err = stream.Read(buf[end : end+4])
	require.NoError(t, err)
	require.Equal(t, 4, n)
	end += n
	require.Equal(t, adler32.Checksum(buf[:end]), stream.digest().Sum32())
}

func TestDecoderStreamWithDigestSetEnabled(t *testing.T) {
	stream := newTestDecoderStream()

	// Disable digest calculation
	stream.setDigestReaderEnabled(false)

	buf := make([]byte, 5)
	_, err := stream.Read(buf)
	require.NoError(t, err)
	require.Equal(t, stream.digest().Sum32(), uint32(1))

	_, err = stream.ReadByte()
	require.NoError(t, err)
	require.Equal(t, stream.digest().Sum32(), uint32(1))

	// Enable digest calculation
	stream.setDigestReaderEnabled(true)

	_, err = stream.Read(buf)
	require.NoError(t, err)
	require.Equal(t, stream.digest().Sum32(), adler32.Checksum([]byte(srcString[6:11])))

	_, err = stream.ReadByte()
	require.NoError(t, err)
	require.Equal(t, stream.digest().Sum32(), adler32.Checksum([]byte(srcString[6:12])))
}

func newTestDecoderStream() *decoderStreamWithDigest {
	d := newDecoderStreamWithDigest(bufio.NewReader(bytes.NewReader([]byte(srcString))))
	d.setDigestReaderEnabled(true)
	return d
}
