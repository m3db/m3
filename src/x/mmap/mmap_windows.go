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

//go:build windows
// +build windows

package mmap

/*
 * This package presents a dummy implementation for Windows that returns not implemented errors.
 * M3DB does not run on Windows, but this dummy implementation will allow packages that transitively
 * include M3 to run on Windows.
 */

import "errors"

// Fd mmaps a file (not implemented on Windows)
func Fd(fd, offset, length int64, opts Options) (Descriptor, error) {
  return Descriptor{}, errors.New("not implemented")
}

// Munmap munmaps a byte slice that is backed by an mmap
// (not implemented on Windows)
func Munmap(desc Descriptor) error {
  return errors.New("not implemented")
}

// MadviseDontNeed frees mmapped memory.
// (not implemented on Windows)
func MadviseDontNeed(desc Descriptor) error {
  return errors.New("not implemented")
}

// Bytes requests a private (non-shared) region of anonymous (not backed by a file) memory from the O.S
// (not implemented on Windows)
func Bytes(length int64, opts Options) (Descriptor, error) {
  return Descriptor{}, errors.New("not implemented")
}
