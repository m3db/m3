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

package agent

import (
	"io"
	"os"
	"path/filepath"

	xerrors "github.com/m3db/m3x/errors"
)

// non-thread safe multi-writer
type multiWriter struct {
	io.Closer
	fds []*os.File
}

// based on signature of os.OpenFile
func newMultiWriter(paths []string, flags int, fileMode os.FileMode, dirMode os.FileMode) (*multiWriter, error) {
	mw := &multiWriter{}
	for _, p := range paths {
		// ensure directory exists
		dir := filepath.Dir(p)
		err := os.MkdirAll(dir, dirMode)
		if err != nil {
			mw.Close() // cleanup
			return nil, err
		}

		// create file
		fd, err := os.OpenFile(p, flags, fileMode)
		if err != nil {
			mw.Close() // cleanup
			return nil, err
		}
		mw.fds = append(mw.fds, fd)
	}
	return mw, nil
}

// mimics signature of os.File.Write
func (mw *multiWriter) write(b []byte) (int, error) {
	// write the provided data to all fds, terminating
	// early if any fd write fails
	for _, fd := range mw.fds {
		n, err := fd.Write(b)
		if err != nil {
			return n, err
		}
	}
	return len(b), nil
}

func (mw *multiWriter) Close() error {
	var me xerrors.MultiError
	for _, fd := range mw.fds {
		me = me.Add(fd.Close())
	}
	return me.FinalError()
}
