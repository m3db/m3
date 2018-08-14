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
	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/x/mmap"
)

// ManagedConcurrentBloomFilter is a container object that implements lifecycle
// management on-top of a BloomFilter. I.E it wraps a bloom filter such that
// all resources are released when the Close() method is called. It's also safe
// for concurrent access
type ManagedConcurrentBloomFilter struct {
	bloomFilter *bloom.ConcurrentReadOnlyBloomFilter
	mmapBytes   []byte
}

// Test tests whether a value is in the bloom filter
func (bf *ManagedConcurrentBloomFilter) Test(value []byte) bool {
	return bf.bloomFilter.Test(value)
}

// M returns the number of elements in the bloom filter
func (bf *ManagedConcurrentBloomFilter) M() uint {
	return bf.bloomFilter.M()
}

// K returns the number of hash functions in the bloom filter
func (bf *ManagedConcurrentBloomFilter) K() uint {
	return bf.bloomFilter.K()
}

// Close closes the bloom filter, releasing any held resources
func (bf *ManagedConcurrentBloomFilter) Close() error {
	return mmap.Munmap(bf.mmapBytes)
}

func newManagedConcurrentBloomFilter(
	bloomFilter *bloom.ConcurrentReadOnlyBloomFilter,
	mmapBytes []byte,
) *ManagedConcurrentBloomFilter {
	return &ManagedConcurrentBloomFilter{
		bloomFilter: bloomFilter,
		mmapBytes:   mmapBytes,
	}
}

func newManagedConcurrentBloomFilterFromFile(
	bloomFilterFd *os.File,
	bloomFilterFdWithDigest digest.FdWithDigestReader,
	expectedDigest uint32,
	numElementsM uint,
	numHashesK uint,
) (*ManagedConcurrentBloomFilter, error) {
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
	result, err := mmap.Bytes(numBytes, mmap.Options{Read: true, Write: true})
	if err != nil {
		return nil, err
	}
	anonMmap := result.Result

	// Validate the bytes on disk using the digest, and read them into
	// the mmap'd region
	_, err = bloomFilterFdWithDigest.ReadAllAndValidate(
		anonMmap, expectedDigest)
	if err != nil {
		mmap.Munmap(anonMmap)
		return nil, err
	}

	bloomFilter := bloom.NewConcurrentReadOnlyBloomFilter(numElementsM, numHashesK, anonMmap)
	return newManagedConcurrentBloomFilter(bloomFilter, anonMmap), nil
}
