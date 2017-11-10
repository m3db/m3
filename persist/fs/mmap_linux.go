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

import "syscall"

const (
	hugePageSize = 2 << 20 // 2mb
)

func mmap(fd, offset, length int, opts mmapOptions) ([]byte, error) {
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

	flags := syscall.MAP_SHARED
	b, err := syscall.Mmap(fd, int64(offset), length, prot, flags)
	if err != nil {
		return nil, err
	}

	if length < hugePageSize {
		return b, nil
	}

	if err := syscall.Madvise(b, syscall.MADV_HUGEPAGE); err != nil {
		return nil, err
	}

	return b, nil
}

func munmap(b []byte) error {
	if len(b) == 0 {
		// Never actually mmapd this, just returned empty slice
		return nil
	}

	return syscall.Munmap(b)
}
