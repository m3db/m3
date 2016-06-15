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

import (
	"github.com/m3db/m3db/bootstrap/bootstrapper"
	"github.com/m3db/m3db/interfaces/m3db"
)

const (
	// FileSystemBootstrapperName is the name of th filesystem bootstrapper.
	FileSystemBootstrapperName = "filesystem"
)

type fileSystemBootstrapper struct {
	m3db.Bootstrapper
}

// NewFileSystemBootstrapper creates a new bootstrapper to bootstrap from on-disk files.
func NewFileSystemBootstrapper(
	prefix string,
	dbOpts m3db.DatabaseOptions,
	next m3db.Bootstrapper,
) m3db.Bootstrapper {
	fss := newFileSystemSource(prefix, dbOpts)
	return &fileSystemBootstrapper{
		Bootstrapper: bootstrapper.NewBaseBootstrapper(fss, dbOpts, next),
	}
}

func (fsb *fileSystemBootstrapper) String() string {
	return FileSystemBootstrapperName
}
