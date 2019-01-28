// Copyright (c) 2019 Uber Technologies, Inc.
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
	"fmt"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/x/mmap"
)

func validateAndMmap(
	fdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
	forceMmapMemory bool,
) ([]byte, error) {

	if forceMmapMemory {
		return validateAndMmapMemory(fdWithDigest, expectedDigest)
	}

	return validateAndMmapFile(fdWithDigest, expectedDigest)

}

func validateAndMmapMemory(
	fdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
) ([]byte, error) {
	fd := fdWithDigest.Fd()
	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}
	numBytes := stat.Size()

	// Request an anonymous (non-file-backed) mmap region. Note that we're going
	// to use the mmap'd region to store the read-only summaries data, but the mmap
	// region itself needs to be writable so we can copy the bytes from disk
	// into it.
	mmapResult, err := mmap.Bytes(numBytes, mmap.Options{Read: true, Write: true})
	if err != nil {
		return nil, err
	}

	mmapBytes := mmapResult.Result

	// Validate the bytes on disk using the digest, and read them into
	// the mmap'd region
	_, err = fdWithDigest.ReadAllAndValidate(mmapBytes, expectedDigest)
	if err != nil {
		mmap.Munmap(mmapBytes)
		return nil, err
	}

	return mmapBytes, nil
}

func validateAndMmapFile(
	fdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
) ([]byte, error) {
	fd := fdWithDigest.Fd()
	mmapResult, err := mmap.File(fd, mmap.Options{Read: true, Write: false})
	if err != nil {
		return nil, err
	}

	mmapBytes := mmapResult.Result
	if calculatedDigest := digest.Checksum(mmapBytes); calculatedDigest != expectedDigest {
		mmap.Munmap(mmapBytes)
		return nil, fmt.Errorf("expected summaries file digest was: %d, but got: %d",
			expectedDigest, calculatedDigest)
	}

	return mmapBytes, nil
}
