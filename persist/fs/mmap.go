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
}

func mmapFiles(opener fileOpener, files map[string]mmapFileDesc) error {
	multiErr := xerrors.NewMultiError()
	for filePath, desc := range files {
		fd, err := opener(filePath)
		if err != nil {
			multiErr = multiErr.Add(err)
			break
		}

		b, err := mmapFile(fd, desc.options)
		if err != nil {
			multiErr = multiErr.Add(err)
			break
		}

		*desc.file = fd
		*desc.bytes = b
	}

	if multiErr.FinalError() == nil {
		return nil
	}

	// If we have encountered an error when opening the files,
	// close the ones that have been opened.
	for _, desc := range files {
		if *desc.file != nil {
			multiErr = multiErr.Add((*desc.file).Close())
		}
		if *desc.bytes != nil {
			multiErr = multiErr.Add(munmap(*desc.bytes))
		}
	}

	return multiErr.FinalError()
}

func mmapFile(file *os.File, opts mmapOptions) ([]byte, error) {
	name := file.Name()
	stat, err := os.Stat(name)
	if err != nil {
		return nil, fmt.Errorf("mmap file could not stat %s: %v", name, err)
	}
	if stat.IsDir() {
		return nil, fmt.Errorf("mmap target is directory: %s", name)
	}
	return mmap(int(file.Fd()), 0, int(stat.Size()), opts)
}
