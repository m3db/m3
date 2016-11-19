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

package pool

import (
	"fmt"
	"syscall"
)

const (
	mmapRegionProtections = syscall.PROT_READ | syscall.PROT_WRITE
	mmapFlags             = syscall.MAP_ANON | syscall.MAP_PRIVATE
)

func mmap(n int) []byte {
	if r, err := syscall.Mmap(
		0, 0, n, mmapRegionProtections, mmapFlags,
	); err != nil {
		panic(fmt.Errorf("off-heap arena acquire error: %v", err))
	} else {
		return r
	}
}

func munmap(p []byte) {
	if err := syscall.Munmap(p); err != nil {
		panic(fmt.Errorf("off-heap arena release error: %v", err))
	}
}
