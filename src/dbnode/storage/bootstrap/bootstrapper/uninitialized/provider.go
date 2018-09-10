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

package uninitialized

import (
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
)

const (
	// UninitializedBootstrapperName is the name of the uninitialized bootstrapper.
	UninitializedBootstrapperName = "uninitialized"
)

type uninitializedBootstrapperProvider struct {
	opts Options
	next bootstrap.BootstrapperProvider
}

// NewUninitializedBootstrapperProvider creates a new uninitialized bootstrapper
// provider.
func NewUninitializedBootstrapperProvider(
	opts Options,
	next bootstrap.BootstrapperProvider,
) (bootstrap.BootstrapperProvider, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	return uninitializedBootstrapperProvider{
		opts: opts,
		next: next,
	}, nil
}

func (p uninitializedBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	var (
		src  = newUninitializedSource(p.opts)
		b    = &uninitializedBootstrapper{}
		next bootstrap.Bootstrapper
		err  error
	)

	if p.next != nil {
		next, err = p.next.Provide()
		if err != nil {
			return nil, err
		}
	}

	return bootstrapper.NewBaseBootstrapper(
		b.String(), src, p.opts.ResultOptions(), next)
}

func (p uninitializedBootstrapperProvider) String() string {
	return UninitializedBootstrapperName
}

type uninitializedBootstrapper struct {
	bootstrap.Bootstrapper
}

func (*uninitializedBootstrapper) String() string {
	return UninitializedBootstrapperName
}
