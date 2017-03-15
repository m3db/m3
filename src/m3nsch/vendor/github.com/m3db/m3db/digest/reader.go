// Copyright (c) 2016 Uber Technologies, Inc.
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

package digest

import (
	"bufio"
	"errors"
	"io"
	"os"
)

var (
	// errReadFewerThanExpectedBytes returned when number of bytes read is fewer than expected
	errReadFewerThanExpectedBytes = errors.New("number of bytes read is fewer than expected")

	// errCheckSumMismatch returned when the calculated checksum doesn't match the stored checksum
	errCheckSumMismatch = errors.New("calculated checksum doesn't match stored checksum")

	// errBufferSizeMismatch returned when ReadAllAndValidate called without well sized buffer
	errBufferSizeMismatch = errors.New("buffer passed is not an exact fit for contents")
)

// FdWithDigestReader provides a buffered reader for reading from the underlying file.
type FdWithDigestReader interface {
	FdWithDigest

	// ReadBytes reads bytes from the underlying file into the provided byte slice.
	ReadBytes(b []byte) (int, error)

	// ReadAllAndValidate reads everything in the underlying file and validates
	// it against the expected digest, returning an error if they don't match.
	// Note: the buffer "b" must be an exact match for how long the contents being
	// read is, the signature is structured this way to allow for buffer reuse.
	ReadAllAndValidate(b []byte, expectedDigest uint32) (int, error)

	// Validate compares the current digest against the expected digest and returns
	// an error if they don't match.
	Validate(expectedDigest uint32) error
}

type fdWithDigestReader struct {
	FdWithDigest

	reader *bufio.Reader
}

// NewFdWithDigestReader creates a new FdWithDigestReader.
func NewFdWithDigestReader(bufferSize int) FdWithDigestReader {
	return &fdWithDigestReader{
		FdWithDigest: newFdWithDigest(),
		reader:       bufio.NewReaderSize(nil, bufferSize),
	}
}

// Reset resets the underlying file descriptor and the buffered reader.
func (r *fdWithDigestReader) Reset(fd *os.File) {
	r.FdWithDigest.Reset(fd)
	r.reader.Reset(fd)
}

func (r *fdWithDigestReader) readBytes(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	if err != nil {
		return 0, err
	}
	// In case the buffered reader only returns what's remaining in
	// the buffer, recursively read what's left in the underlying reader.
	if n < len(b) {
		b = b[n:]
		remainder, err := r.readBytes(b)
		return n + remainder, err
	}
	return n, err
}

func (r *fdWithDigestReader) ReadBytes(b []byte) (int, error) {
	n, err := r.readBytes(b)
	if err != nil && err != io.EOF {
		return n, err
	}
	// If we encountered an EOF error and didn't read any bytes
	// given a non-empty slice, we return an EOF error.
	if err == io.EOF && n == 0 && len(b) > 0 {
		return 0, err
	}
	if _, err := r.FdWithDigest.Digest().Write(b[:n]); err != nil {
		return 0, err
	}
	return n, nil
}

func (r *fdWithDigestReader) ReadAllAndValidate(b []byte, expectedDigest uint32) (int, error) {
	n, err := r.readBytes(b)
	if err != nil {
		return n, err
	}
	_, err = r.reader.ReadByte()
	if err != io.EOF {
		return 0, errBufferSizeMismatch
	}
	err = nil
	if _, err := r.FdWithDigest.Digest().Write(b); err != nil {
		return n, err
	}
	if err := r.Validate(expectedDigest); err != nil {
		return n, err
	}
	return n, nil
}

func (r *fdWithDigestReader) Validate(expectedDigest uint32) error {
	if r.FdWithDigest.Digest().Sum32() != expectedDigest {
		return errCheckSumMismatch
	}
	return nil
}

// FdWithDigestContentsReader provides additional functionality of reading a digest from the underlying file.
type FdWithDigestContentsReader interface {
	FdWithDigestReader

	// ReadDigest reads a digest from the underlying file.
	ReadDigest() (uint32, error)
}

type fdWithDigestContentsReader struct {
	FdWithDigestReader

	digestBuf Buffer
}

// NewFdWithDigestContentsReader creates a new FdWithDigestContentsReader.
func NewFdWithDigestContentsReader(bufferSize int) FdWithDigestContentsReader {
	return &fdWithDigestContentsReader{
		FdWithDigestReader: NewFdWithDigestReader(bufferSize),
		digestBuf:          NewBuffer(),
	}
}

func (r *fdWithDigestContentsReader) ReadDigest() (uint32, error) {
	n, err := r.ReadBytes(r.digestBuf)
	if err != nil {
		return 0, err
	}
	if n < len(r.digestBuf) {
		return 0, errReadFewerThanExpectedBytes
	}
	return r.digestBuf.ReadDigest(), nil
}
