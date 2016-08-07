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
	"hash"
	"os"
)

// fdWithDigest is a wrapper around a file descriptor
// alongside the digest for what's stored in the file.
type fdWithDigest struct {
	fd     *os.File
	digest hash.Hash32
}

func newFdWithDigest() *fdWithDigest {
	return &fdWithDigest{
		digest: NewDigest(),
	}
}

// Fd returns the file descriptor.
func (fwd *fdWithDigest) Fd() *os.File {
	return fwd.fd
}

// Digest returns the digest.
func (fwd *fdWithDigest) Digest() uint32 {
	return fwd.digest.Sum32()
}

// Reset resets the file descriptor and the digest.
func (fwd *fdWithDigest) Reset(fd *os.File) {
	fwd.fd = fd
	fwd.digest.Reset()
}

// Close closes the file descriptor.
func (fwd *fdWithDigest) Close() error {
	fd := fwd.fd
	fwd.fd = nil
	return fd.Close()
}
