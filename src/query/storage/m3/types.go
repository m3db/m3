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

package m3

import (
	"context"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	genericstorage "github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/x/instrument"
	xresource "github.com/m3db/m3/src/x/resource"
)

// Cleanup is a cleanup function to be called after resources are freed.
type Cleanup func() error

func noop() error {
	return nil
}

// Storage provides an interface for reading and writing to the TSDB.
type Storage interface {
	genericstorage.Storage
	Querier
}

// Querier handles queries against an M3 instance.
type Querier interface {
	// FetchCompressedResult fetches timeseries data based on a query.
	FetchCompressedResult(
		ctx context.Context,
		query *genericstorage.FetchQuery,
		options *genericstorage.FetchOptions,
	) (consolidators.SeriesFetchResult, Cleanup, error)

	// SearchCompressed fetches matching tags based on a query.
	SearchCompressed(
		ctx context.Context,
		query *genericstorage.FetchQuery,
		options *genericstorage.FetchOptions,
	) (consolidators.TagResult, Cleanup, error)

	// CompleteTagsCompressed returns autocompleted tag results.
	CompleteTagsCompressed(
		ctx context.Context,
		query *genericstorage.CompleteTagsQuery,
		options *genericstorage.FetchOptions,
	) (*consolidators.CompleteTagsResult, error)
}

// DynamicClusterNamespaceConfiguration is the configuration for
// dynamically fetching namespace configuration.
type DynamicClusterNamespaceConfiguration struct {
	// session is an active session connected to an M3DB cluster.
	session client.Session

	// nsInitializer is the initializer used to watch for namespace changes.
	nsInitializer namespace.Initializer
}

// DynamicClusterOptions is the options for a new dynamic Cluster.
type DynamicClusterOptions interface {
	// Validate validates the DynamicClusterOptions.
	Validate() error

	// SetDynamicClusterNamespaceConfiguration sets the configuration for the dynamically fetching cluster namespaces.
	SetDynamicClusterNamespaceConfiguration(value []DynamicClusterNamespaceConfiguration) DynamicClusterOptions

	// SetDynamicClusterNamespaceConfiguration returns the configuration for the dynamically fetching cluster namespaces.
	DynamicClusterNamespaceConfiguration() []DynamicClusterNamespaceConfiguration

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) DynamicClusterOptions

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetClusterNamespacesWatcher sets the namespaces watcher which alerts components that
	// need to regenerate configuration when the ClusterNamespaces change.
	SetClusterNamespacesWatcher(value ClusterNamespacesWatcher) DynamicClusterOptions

	// ClusterNamespacesWatcher returns the namespaces watcher which alerts components that
	// need to regenerate configuration when the ClusterNamespaces change.
	ClusterNamespacesWatcher() ClusterNamespacesWatcher
}

// ClusterNamespacesWatcher allows interested parties to watch for changes
// to the cluster namespaces and register callbacks to be invoked
// when changes are detected.
type ClusterNamespacesWatcher interface {
	// Update updates the current namespaces.
	Update(namespaces ClusterNamespaces) error

	// Get returns the current namespaces.
	Get() ClusterNamespaces

	// RegisterListener registers a listener for updates to cluster namespaces.
	// If a value is currently present, it will synchronously call back the listener.
	RegisterListener(listener ClusterNamespacesListener) xresource.SimpleCloser

	// Close closes the watcher and all descendent watches.
	Close()
}

// ClusterNamespacesListener is a listener for receiving updates from a
// ClusterNamespacesWatcher.
type ClusterNamespacesListener interface {
	// OnUpdate is called when updates have occurred passing in the new namespaces.
	OnUpdate(namespaces ClusterNamespaces)
}
