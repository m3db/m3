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
	reporterOptions mmap.ReporterOptions,
) (mmap.Descriptor, error) {
	if forceMmapMemory {
		return validateAndMmapMemory(fdWithDigest, expectedDigest, reporterOptions)
	}

	return validateAndMmapFile(fdWithDigest, expectedDigest, reporterOptions)
}

func validateAndMmapMemory(
	fdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
	reporterOptions mmap.ReporterOptions,
) (mmap.Descriptor, error) {
	fd := fdWithDigest.Fd()
	stat, err := fd.Stat()
	if err != nil {
		return mmap.Descriptor{}, err
	}
	numBytes := stat.Size()

	// Request an anonymous (non-file-backed) mmap region. Note that we're going
	// to use the mmap'd region to store the read-only summaries data, but the mmap
	// region itself needs to be writable so we can copy the bytes from disk
	// into it.
	mmapDescriptor, err := mmap.Bytes(numBytes, mmap.Options{Read: true, Write: true, ReporterOptions: reporterOptions})
	if err != nil {
		return mmap.Descriptor{}, err
	}

	// Validate the bytes on disk using the digest, and read them into
	// the mmap'd region
	_, err = fdWithDigest.ReadAllAndValidate(mmapDescriptor.Bytes, expectedDigest)
	if err != nil {
		mmap.Munmap(mmapDescriptor)
		return mmap.Descriptor{}, err
	}

	return mmapDescriptor, nil
}

func validateAndMmapFile(
	fdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
	reporterOptions mmap.ReporterOptions,
) (mmap.Descriptor, error) {
	fd := fdWithDigest.Fd()
	mmapDescriptor, err := mmap.File(fd, mmap.Options{Read: true, Write: false, ReporterOptions: reporterOptions})
	if err != nil {
		return mmap.Descriptor{}, err
	}

	if calculatedDigest := digest.Checksum(mmapDescriptor.Bytes); calculatedDigest != expectedDigest {
		mmap.Munmap(mmapDescriptor)
		return mmap.Descriptor{}, fmt.Errorf("expected %s file digest was: %d, but got: %d",
			fd.Name(), expectedDigest, calculatedDigest)
	}

	return mmapDescriptor, nil
}
