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

// +build !linux

package fs

import (
	"fmt"
	"syscall"
)

// mmap mmaps a file
func mmap(fd, offset, length int64, opts mmapOptions) ([]byte, error) {
	// MAP_SHARED because we want writes to the byte slice to be reflected back in the underlying file
	return mmapBase(fd, offset, length, syscall.MAP_SHARED, opts)
}

// mmapAnon requests a private (non-shared) region of anonymous (not backed by a file) memory from the O.S
func mmapAnon(length int64, opts mmapOptions) ([]byte, error) {
	// offset is 0 because we're not indexing into a file
	// fd is -1 and MAP_ANON because we're asking for an anonymous region of memory not tied to a file
	// MAP_PRIVATE because we don't plan on sharing this region of memory with other processes
	return mmapBase(-1, 0, length, syscall.MAP_ANON|syscall.MAP_PRIVATE, opts)
}

func mmapBase(fd, offset, length int64, flags int, opts mmapOptions) ([]byte, error) {
	if length == 0 {
		// Return an empty slice (but not nil so callers who
		// use nil to mean something special like not initialized
		// get back an actual ref)
		return make([]byte, 0), nil
	}

	var prot int
	if opts.read {
		prot = prot | syscall.PROT_READ
	}
	if opts.write {
		prot = prot | syscall.PROT_WRITE
	}

	b, err := syscall.Mmap(int(fd), offset, int(length), prot, flags)
	if err != nil {
		return nil, fmt.Errorf("mmap error: %v", err)
	}

	return b, nil
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
