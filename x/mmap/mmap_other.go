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

package mmap

import (
	"fmt"
	"syscall"
)

// Fd mmaps a file
func Fd(fd, offset, length int64, opts Options) (Result, error) {
	// MAP_PRIVATE because we only want to ever mmap immutable things and we don't
	// ever want to propagate writes back to the underlying file
	// Set HugeTLB to disabled because its not supported for files
	opts.HugeTLB.Enabled = false
	return mmap(fd, offset, length, syscall.MAP_PRIVATE, opts)
}

// Bytes requests a private (non-shared) region of anonymous (not backed by a file) memory from the O.S
func Bytes(length int64, opts Options) (Result, error) {
	// offset is 0 because we're not indexing into a file
	// fd is -1 and MAP_ANON because we're asking for an anonymous region of memory not tied to a file
	// MAP_PRIVATE because we don't plan on sharing this region of memory with other processes
	return mmap(-1, 0, length, syscall.MAP_ANON|syscall.MAP_PRIVATE, opts)
}

func mmap(fd, offset, length int64, flags int, opts Options) (Result, error) {
	if length == 0 {
		// Return an empty slice (but not nil so callers who
		// use nil to mean something special like not initialized
		// get back an actual ref)
		return Result{Result: make([]byte, 0)}, nil
	}

	var prot int
	if opts.Read {
		prot = prot | syscall.PROT_READ
	}
	if opts.Write {
		prot = prot | syscall.PROT_WRITE
	}

	b, err := syscall.Mmap(int(fd), offset, int(length), prot, flags)
	if err != nil {
		return Result{}, fmt.Errorf("mmap error: %v", err)
	}

	return Result{Result: b}, nil
}

// Munmap munmaps a byte slice that is backed by an mmap
func Munmap(b []byte) error {
	if len(b) == 0 {
		// Never actually mmapd this, just returned empty slice
		return nil
	}

	if err := syscall.Munmap(b); err != nil {
		return fmt.Errorf("munmap error: %v", err)
	}

	return nil
}
