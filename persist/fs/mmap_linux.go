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
	"fmt"
	"syscall"
)

// mmapFd mmaps a file
func mmapFd(fd, offset, length int64, opts mmapOptions) (mmapResult, error) {
	// MAP_PRIVATE because we only want to ever mmap immutable things and we don't
	// ever want to propagate writes back to the underlying file
	return mmap(fd, offset, length, syscall.MAP_PRIVATE, opts)
}

// mmapBytes requests a private (non-shared) region of anonymous (not backed by a file) memory from the O.S
func mmapBytes(length int64, opts mmapOptions) (mmapResult, error) {
	// offset is 0 because we're not indexing into a file
	// fd is -1 and MAP_ANON because we're asking for an anonymous region of memory not tied to a file
	// MAP_PRIVATE because we don't plan on sharing this region of memory with other processes
	return mmap(-1, 0, length, syscall.MAP_ANON|syscall.MAP_PRIVATE, opts)
}

func mmap(fd, offset, length int64, flags int, opts mmapOptions) (mmapResult, error) {
	if length == 0 {
		// Return an empty slice (but not nil so callers who
		// use nil to mean something special like not initialized
		// get back an actual ref)
		return mmapResult{result: make([]byte, 0)}, nil
	}

	var prot int
	if opts.read {
		prot = prot | syscall.PROT_READ
	}
	if opts.write {
		prot = prot | syscall.PROT_WRITE
	}

	flagsWithoutHugeTLB := flags
	shouldUseHugeTLB := opts.hugeTLB.enabled && length >= opts.hugeTLB.threshold
	if shouldUseHugeTLB {
		// We use the MAP_HUGETLB flag instead of MADV_HUGEPAGE because transparent
		// hugepages only work with anonymous, private pages. Please see the MADV_HUGEPAGE
		// section of http://man7.org/linux/man-pages/man2/madvise.2.html and the MAP_HUGETLB
		// section of http://man7.org/linux/man-pages/man2/mmap.2.html for more details.
		flags = flags | syscall.MAP_HUGETLB
	}

	var (
		b          []byte
		err        error
		warning    error
		withTLBErr error
	)
	b, err = syscall.Mmap(int(fd), offset, int(length), prot, flags)
	// Save incase we need to include it in the warning later
	withTLBErr = err
	// Sometimes allocations that specify huge pages will fail because the O.S
	// isn't configured properly or there are not enough available huge pages in
	// the pool. You can try and allocate more by executing:
	// 		echo 20 > /proc/sys/vm/nr_hugepages
	// See this document for more details: https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt
	// Regardless, we don't want to fail hard in that scenario. Instead, we try
	// and mmap without the hugeTLB flag.
	if err != nil && shouldUseHugeTLB {
		b, err = syscall.Mmap(int(fd), offset, int(length), prot, flagsWithoutHugeTLB)
		// If we succeeded the second time, then proceed but make sure the caller
		// receives a warning that includes the error from when we tried to use the
		// hugeTLB flag.
		if err == nil {
			warning = fmt.Errorf(
				"error while trying to mmap with hugeTLB flag: %s, hugeTLB disabled", withTLBErr.Error())
		}
	}

	if err != nil {
		return mmapResult{}, fmt.Errorf("mmap error: %v", err)
	}

	return mmapResult{result: b}, nil
}

func munmap(b []byte) error {
	if len(b) == 0 {
		// Never actually mmapd this, just returned empty slice
		return nil
	}

	if err := syscall.Munmap(b); err != nil {
		return fmt.Errorf("munmap error: %v", err)
	}

	return nil
}
