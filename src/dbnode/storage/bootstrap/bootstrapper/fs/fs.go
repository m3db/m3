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

// Package fs implements file system bootstrapping.
package fs

import (
	"fmt"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
)

const (
	// FileSystemBootstrapperName is the name of th filesystem bootstrapper.
	FileSystemBootstrapperName = "filesystem"
)

type fileSystemBootstrapperProvider struct {
	opts Options
	next bootstrap.BootstrapperProvider
}

// NewFileSystemBootstrapperProvider creates a new bootstrapper to bootstrap from on-disk files.
func NewFileSystemBootstrapperProvider(
	opts Options,
	next bootstrap.BootstrapperProvider,
) (bootstrap.BootstrapperProvider, error) {
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("unable to validate fs options: %v", err)
	}
	return fileSystemBootstrapperProvider{
		opts: opts,
		next: next,
	}, nil
}

func (p fileSystemBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	var (
		b    = &fileSystemBootstrapper{}
		next bootstrap.Bootstrapper
	)
	src, err := newFileSystemSource(p.opts)
	if err != nil {
		return nil, err
	}

	if p.next != nil {
		next, err = p.next.Provide()
		if err != nil {
			return nil, err
		}
	}
	return bootstrapper.NewBaseBootstrapper(b.String(),
		src, p.opts.ResultOptions(), next)
}

func (p fileSystemBootstrapperProvider) String() string {
	return FileSystemBootstrapperName
}

type fileSystemBootstrapper struct {
	bootstrap.Bootstrapper
}

func (fsb *fileSystemBootstrapper) String() string {
	return FileSystemBootstrapperName
}
