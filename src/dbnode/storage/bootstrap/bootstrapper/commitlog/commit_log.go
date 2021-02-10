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

// Package commitlog implements commit log bootstrapping.
package commitlog

import (
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
)

const (
	// CommitLogBootstrapperName is the name of the commit log bootstrapper.
	CommitLogBootstrapperName = "commitlog"
)

type commitLogBootstrapperProvider struct {
	opts       Options
	inspection fs.Inspection
	next       bootstrap.BootstrapperProvider
}

// NewCommitLogBootstrapperProvider creates a new bootstrapper provider
// to bootstrap from commit log files.
func NewCommitLogBootstrapperProvider(
	opts Options,
	inspection fs.Inspection,
	next bootstrap.BootstrapperProvider,
) (bootstrap.BootstrapperProvider, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return commitLogBootstrapperProvider{
		opts:       opts,
		inspection: inspection,
		next:       next,
	}, nil
}

func (p commitLogBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	var (
		src  = newCommitLogSource(p.opts, p.inspection)
		b    = &commitLogBootstrapper{}
		next bootstrap.Bootstrapper
		err  error
	)
	if p.next != nil {
		next, err = p.next.Provide()
		if err != nil {
			return nil, err
		}
	}
	return bootstrapper.NewBaseBootstrapper(b.String(),
		src, p.opts.ResultOptions(), next)
}

func (p commitLogBootstrapperProvider) String() string {
	return CommitLogBootstrapperName
}

type commitLogBootstrapper struct {
	bootstrap.Bootstrapper
}

func (*commitLogBootstrapper) String() string {
	return CommitLogBootstrapperName
}
