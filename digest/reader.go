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
	"io/ioutil"
	"os"
)

var (
	// errCheckSumMismatch returned when the calculated checksum doesn't match the stored checksum
	errCheckSumMismatch = errors.New("calculated checksum doesn't match stored checksum")
)

// FdWithDigestReader provides a buffered reader for reading from the underlying file.
type FdWithDigestReader interface {
	FdWithDigest

	// ReadBytes reads bytes from the underlying file into the provided byte slice.
	ReadBytes(b []byte) (int, error)

	// ReadAllAndValidate reads everything in the underlying file and validates
	// it against the expected digest, returning an error if they don't match.
	ReadAllAndValidate(expectedDigest uint32) ([]byte, error)

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
	if err != nil {
		return 0, err
	}
	if _, err := r.FdWithDigest.Digest().Write(b); err != nil {
		return 0, err
	}
	return n, nil
}

func (r *fdWithDigestReader) ReadAllAndValidate(expectedDigest uint32) ([]byte, error) {
	b, err := ioutil.ReadAll(r.reader)
	if err != nil {
		return nil, err
	}
	if _, err := r.FdWithDigest.Digest().Write(b); err != nil {
		return nil, err
	}
	if err := r.Validate(expectedDigest); err != nil {
		return nil, err
	}
	return b, nil
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
	_, err := r.ReadBytes(r.digestBuf)
	if err != nil {
		return 0, err
	}
	return r.digestBuf.ReadDigest(), nil
}
