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

package bootstrapper

import (
	"fmt"

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/context"
)

const (
	// NoOpNoneBootstrapperName is the name of the noOpNoneBootstrapper
	NoOpNoneBootstrapperName = "noop-none"

	// NoOpAllBootstrapperName is the name of the noOpAllBootstrapperProvider
	NoOpAllBootstrapperName = "noop-all"
)

// noOpNoneBootstrapperProvider is the no-op bootstrapper provider that doesn't
// know how to bootstrap any time ranges.
type noOpNoneBootstrapperProvider struct{}

// NewNoOpNoneBootstrapperProvider creates a new noOpNoneBootstrapper.
func NewNoOpNoneBootstrapperProvider() bootstrap.BootstrapperProvider {
	return noOpNoneBootstrapperProvider{}
}

func (noop noOpNoneBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	return noOpNoneBootstrapper{}, nil
}

func (noop noOpNoneBootstrapperProvider) String() string {
	return NoOpNoneBootstrapperName
}

// Compilation of this statement ensures struct implements the interface.
var _ bootstrap.Bootstrapper = noOpNoneBootstrapper{}

type noOpNoneBootstrapper struct{}

func (noop noOpNoneBootstrapper) String() string {
	return NoOpNoneBootstrapperName
}

func (noop noOpNoneBootstrapper) Bootstrap(
	_ context.Context,
	namespaces bootstrap.Namespaces,
	_ bootstrap.Cache,
) (bootstrap.NamespaceResults, error) {
	results := bootstrap.NewNamespaceResults(namespaces)
	for _, elem := range results.Results.Iter() {
		id := elem.Key()
		namespace := elem.Value()

		requested, ok := namespaces.Namespaces.Get(id)
		if !ok {
			return bootstrap.NamespaceResults{},
				fmt.Errorf("missing request namespace: %s", id.String())
		}

		// Set everything as unfulfilled.
		shardTimeRanges := requested.DataRunOptions.ShardTimeRanges
		namespace.DataResult = shardTimeRanges.ToUnfulfilledDataResult()

		if namespace.Metadata.Options().IndexOptions().Enabled() {
			shardTimeRanges := requested.IndexRunOptions.ShardTimeRanges
			namespace.IndexResult = shardTimeRanges.ToUnfulfilledIndexResult()
		}

		// Set value after modifications (map is by value).
		results.Results.Set(id, namespace)
	}

	return results, nil
}

// noOpAllBootstrapperProvider is the no-op bootstrapper provider that pretends
// it can bootstrap any time ranges.
type noOpAllBootstrapperProvider struct{}

// NewNoOpAllBootstrapperProvider creates a new noOpAllBootstrapperProvider.
func NewNoOpAllBootstrapperProvider() bootstrap.BootstrapperProvider {
	return noOpAllBootstrapperProvider{}
}

func (noop noOpAllBootstrapperProvider) Provide() (bootstrap.Bootstrapper, error) {
	return noOpAllBootstrapper{}, nil
}

func (noop noOpAllBootstrapperProvider) String() string {
	return NoOpAllBootstrapperName
}

// Compilation of this statement ensures struct implements the interface.
var _ bootstrap.Bootstrapper = noOpAllBootstrapper{}

type noOpAllBootstrapper struct{}

func (noop noOpAllBootstrapper) String() string {
	return NoOpAllBootstrapperName
}

func (noop noOpAllBootstrapper) Bootstrap(
	_ context.Context,
	namespaces bootstrap.Namespaces,
	_ bootstrap.Cache,
) (bootstrap.NamespaceResults, error) {
	return bootstrap.NewNamespaceResults(namespaces), nil
}
