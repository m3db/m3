// Copyright (c) 2018 Uber Technologies, Inc.
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

package server

import (
	"os"
	paths "path"

	"github.com/pkg/errors"
	"golang.org/x/sys/unix"
)

// lockfile represents an acquired lockfile.
type lockfile struct {
	file os.File
}

// acquireLockfile creates the given file path if it doesn't exist and
// obtains an exclusive lock on it. An error is returned if the lock
// has been obtained by another process.
func acquireLockfile(path string) (*lockfile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "failed opening lock path")
	}

	ft := &unix.Flock_t{
		Pid:  int32(os.Getpid()),
		Type: unix.F_WRLCK,
	}

	if err = unix.FcntlFlock(file.Fd(), unix.F_SETLK, ft); err != nil {
		return nil, errors.Wrap(err, "failed obtaining lock")
	}

	lf := lockfile{*file}

	return &lf, nil
}

// createAndAcquireLockfile creates any non-existing directories needed to
// create the lock file, then acquires a lock on it.
func createAndAcquireLockfile(path string, newDirMode os.FileMode) (*lockfile, error) {
	if err := os.MkdirAll(paths.Dir(path), newDirMode); err != nil {
		return nil, err
	}

	return acquireLockfile(path)
}

// releaseLockfile releases the lock on the file and removes the file.
func (lf lockfile) releaseLockfile() error {
	ft := &unix.Flock_t{
		Pid:  int32(os.Getpid()),
		Type: unix.F_UNLCK,
	}

	if err := unix.FcntlFlock(lf.file.Fd(), unix.F_SETLK, ft); err != nil {
		return errors.Wrap(err, "failed releasing lock")
	}

	if err := lf.file.Close(); err != nil {
		return errors.Wrap(err, "failed closing lock file descriptor")
	}

	if err := os.Remove(lf.file.Name()); err != nil {
		return errors.Wrap(err, "failed removing lock file")
	}

	return nil
}
