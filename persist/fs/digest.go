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

package fs

import (
	"errors"
	"hash"
	"hash/adler32"
	"io/ioutil"
	"os"
)

var (
	// errReadFewerThanExpectedBytes returned when number of bytes read is fewer than expected
	errReadFewerThanExpectedBytes = errors.New("number of bytes read is fewer than expected")

	// errCheckSumMismatch returned when the calculated checksum doesn't match the stored checksum
	errCheckSumMismatch = errors.New("calculated checksum doesn't match stored checksum")
)

type fdWithDigest struct {
	fd     *os.File
	digest hash.Hash32
}

func newFdWithDigest() fdWithDigest {
	return fdWithDigest{digest: adler32.New()}
}

func (fwd fdWithDigest) resetDigest() {
	fwd.digest.Reset()
}

func (fwd fdWithDigest) writeBytes(b []byte) (int, error) {
	written, err := fwd.fd.Write(b)
	if err != nil {
		return 0, err
	}
	if _, err := fwd.digest.Write(b); err != nil {
		return 0, err
	}
	return written, nil
}

// NB(xichen): buf has digestLen bytes.
func (fwd fdWithDigest) writeDigests(buf []byte, digests ...hash.Hash32) error {
	for _, digest := range digests {
		writeDigest(digest, buf)
		if _, err := fwd.writeBytes(buf); err != nil {
			return err
		}
	}
	return nil
}

func (fwd fdWithDigest) readBytes(b []byte) (int, error) {
	n, err := fwd.fd.Read(b)
	if err != nil {
		return 0, err
	}
	if _, err := fwd.digest.Write(b); err != nil {
		return 0, err
	}
	return n, nil
}

func (fwd fdWithDigest) readAllAndValidate(expectedDigest uint32) ([]byte, error) {
	b, err := ioutil.ReadAll(fwd.fd)
	if err != nil {
		return nil, err
	}
	if _, err := fwd.digest.Write(b); err != nil {
		return nil, err
	}
	if err := validateDigest(expectedDigest, fwd.digest); err != nil {
		return nil, err
	}
	return b, nil
}

// NB(xichen): buf has digestLen bytes to avoid memory allocation.
func (fwd fdWithDigest) readDigest(buf []byte) (uint32, error) {
	n, err := fwd.readBytes(buf)
	return toDigest(buf, n, err)
}

func writeDigest(digest hash.Hash32, buf []byte) {
	endianness.PutUint32(buf, digest.Sum32())
}

// NB(xichen): buf has digestLen bytes.
func writeDigestToFile(fd *os.File, digest hash.Hash32, buf []byte) error {
	writeDigest(digest, buf)
	if _, err := fd.Write(buf); err != nil {
		return err
	}
	return nil
}

// NB(xichen): buf has digestLen bytes.
func readDigest(buf []byte) uint32 {
	return endianness.Uint32(buf)
}

// NB(xichen): buf has digestLen bytes.
func readDigestFromFile(fd *os.File, buf []byte) (uint32, error) {
	n, err := fd.Read(buf)
	return toDigest(buf, n, err)
}

func toDigest(buf []byte, n int, err error) (uint32, error) {
	if err != nil {
		return 0, err
	}
	if n < digestLen {
		return 0, errReadFewerThanExpectedBytes
	}
	digest := readDigest(buf)
	return digest, nil
}

func validateDigest(expectedDigest uint32, actualDigest hash.Hash32) error {
	if expectedDigest != actualDigest.Sum32() {
		return errCheckSumMismatch
	}
	return nil
}
