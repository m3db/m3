// Copyright (c) 2017 Uber Technologies, Inc.
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
	"os"

	"github.com/m3db/bloom"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/storage/block"
)

func readBloomFilter(
	bloomFilterFd *os.File,
	bloomFilterFdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
	numElementsM uint,
	numHashesK uint,
) (block.ManagedBloomFilter, error) {
	// Determine how many bytes to request for the mmap'd region
	bloomFilterFdWithDigest.Reset(bloomFilterFd)
	stat, err := bloomFilterFd.Stat()
	if err != nil {
		return nil, err
	}
	numBytes := stat.Size()

	// Request an anonymous (non-file-backed) mmap region. Note that we're going
	// to use the mmap'd region to create a read-only bloom filter, but the mmap
	// region itself needs to be writable so we can copy the bytes from disk
	// into it
	anonMmap, err := mmapAnon(numBytes, mmapOptions{read: true, write: true})
	if err != nil {
		return nil, err
	}

	// Validate the bytes on disk using the digest, and read them into
	// the mmap'd region
	_, err = bloomFilterFdWithDigest.ReadAllAndValidate(
		anonMmap, expectedDigest)
	if err != nil {
		return nil, err
	}

	bloomFilter := bloom.NewConcurrentReadOnlyBloomFilter(numElementsM, numHashesK, anonMmap)
	closeFn := func() error {
		return munmap(anonMmap)
	}

	return block.NewManagedBloomFilter(bloomFilter, closeFn), nil
}
