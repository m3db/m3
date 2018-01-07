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
	"os"

	xerrors "github.com/m3db/m3x/errors"
)

type mmapFileDesc struct {
	// file is the *os.File ref to store
	file **os.File
	// bytes is the []byte slice ref to store the mmap'd address
	bytes *[]byte
	// options specifies options to use when mmaping a file
	options mmapOptions
}

type mmapOptions struct {
	// read is whether to make mmap bytes ref readable
	read bool
	// write is whether to make mmap bytes ref writable
	write bool
	// hugeTLB is the mmap huge TLB options
	hugeTLB mmapHugeTLBOptions
}

type mmapResult struct {
	result  []byte
	warning error
}

type mmapHugeTLBOptions struct {
	// enabled determines if using the huge TLB flag is enabled for platforms
	// that support it
	enabled bool
	// threshold determines if the size being mmap'd is greater or equal
	// to this value to use or not use the huge TLB flag if enabled
	threshold int64
}

type mmapFilesResult struct {
	warning error
}

func mmapFiles(opener fileOpener, files map[string]mmapFileDesc) (mmapFilesResult, error) {
	multiWarn := xerrors.NewMultiError()
	multiErr := xerrors.NewMultiError()

	for filePath, desc := range files {
		fd, err := opener(filePath)
		if err != nil {
			multiErr = multiErr.Add(errorWithFilename(filePath, err))
			break
		}

		result, err := mmapFile(fd, desc.options)
		if err != nil {
			multiErr = multiErr.Add(errorWithFilename(filePath, err))
			break
		}
		if result.warning != nil {
			multiWarn = multiWarn.Add(errorWithFilename(filePath, result.warning))
			break
		}

		*desc.file = fd
		*desc.bytes = result.result
	}

	if multiErr.FinalError() == nil {
		return mmapFilesResult{warning: multiWarn.FinalError()}, nil
	}

	// If we have encountered an error when opening the files,
	// close the ones that have been opened.
	for filePath, desc := range files {
		if *desc.file != nil {
			multiErr = multiErr.Add(errorWithFilename(filePath, (*desc.file).Close()))
		}
		if *desc.bytes != nil {
			multiErr = multiErr.Add(errorWithFilename(filePath, munmap(*desc.bytes)))
		}
	}

	return mmapFilesResult{warning: multiWarn.FinalError()}, multiErr.FinalError()
}

func mmapFile(file *os.File, opts mmapOptions) (mmapResult, error) {
	name := file.Name()
	stat, err := os.Stat(name)
	if err != nil {
		return mmapResult{}, fmt.Errorf("mmap file could not stat %s: %v", name, err)
	}
	if stat.IsDir() {
		return mmapResult{}, fmt.Errorf("mmap target is directory: %s", name)
	}
	return mmapFd(int64(file.Fd()), 0, stat.Size(), opts)
}

func errorWithFilename(name string, err error) error {
	return fmt.Errorf("file %s encountered err: %s", name, err.Error())
}
