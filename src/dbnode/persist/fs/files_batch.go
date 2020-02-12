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

package fs

type FileDeleter interface {
	Delete(pattern string)
}

// FileDeleter holds reusable memory for filenames to be deleted. This allows
// batch file deletions to be performedf with minimal memory usage.
type BatchFileDeleter struct {
	// The actual full byte slice of all file names concatenated.
	rawFileNames []byte
	// Pointers to individual file names within the raw byte slice.
	fileNames [][]byte
}

// New returns a new deleter with a set of bytes to use for batch deletions.
func New(bytes int) FileDeleter {
	return FileDeleter{
		rawFileNames: make([]byte, 0, bytes),
		fileNames:    [][]byte{},
	}
}

// Glob populates current file batch contents given a file path pattern.
func (fb *FileBatch) Glob(pattern string) error {
	return nil
}

// DeleteAll deletes all of the files in the batch.
func (fb *FileBatch) Delete(pattern string, BatchSize) error {
	return nil
}
