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

package namespace

import (
	"time"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/instrument"
)

// Options controls namespace behavior
// TODO: Rename these to use should instead of need
type Options interface {
	// Validate validates the options
	Validate() error

	// Equal returns true if the provide value is equal to this one
	Equal(value Options) bool

	// SetNeedsBootstrap sets whether this namespace requires bootstrapping
	SetNeedsBootstrap(value bool) Options

	// NeedsBootstrap returns whether this namespace requires bootstrapping
	NeedsBootstrap() bool

	// SetNeedsFlush sets whether the in-memory data for this namespace needs to be flushed
	SetNeedsFlush(value bool) Options

	// NeedsFlush returns whether the in-memory data for this namespace needs to be flushed
	NeedsFlush() bool

	// SetNeedsSnapshot sets whether the in-memory data for this namespace should be snapshotted regularly
	SetNeedsSnapshot(value bool) Options

	// NeedsSnapshot returns whether the in-memory data for this namespace should be snapshotted regularly
	NeedsSnapshot() bool

	// SetWritesToCommitLog sets whether writes for series in this namespace need to go to commit log
	SetWritesToCommitLog(value bool) Options

	// WritesToCommitLog returns whether writes for series in this namespace need to go to commit log
	WritesToCommitLog() bool

	// SetNeedsFilesetCleanup sets whether this namespace requires cleaning up fileset files
	SetNeedsFilesetCleanup(value bool) Options

	// NeedsFilesetCleanup returns whether this namespace requires cleaning up fileset files
	NeedsFilesetCleanup() bool

	// SetNeedsSnapshotCleanup sets whether this namespace requires cleaning up snapshot files
	SetNeedsSnapshotCleanup(value bool) Options

	// NeedsSnapshotCleanup returns whether this namespace reuqires cleaning up snapshot files
	NeedsSnapshotCleanup() bool

	// SetNeedsRepair sets whether the data for this namespace needs to be repaired
	SetNeedsRepair(value bool) Options

	// NeedsRepair returns whether the data for this namespace needs to be repaired
	NeedsRepair() bool

	// SetRetentionOptions sets the retention options for this namespace
	SetRetentionOptions(value retention.Options) Options

	// RetentionOptions returns the retention options for this namespace
	RetentionOptions() retention.Options
}

// Metadata represents namespace metadata information
type Metadata interface {
	// Equal returns true if the provide value is equal to this one
	Equal(value Metadata) bool

	// ID is the ID of the namespace
	ID() ident.ID

	// Options is the namespace options
	Options() Options
}

// Map is mapping from known namespaces' ID to their Metadata
type Map interface {
	// Equal returns true if the provide value is equal to this one
	Equal(value Map) bool

	// Get gets the metadata for the provided namespace
	Get(ident.ID) (Metadata, error)

	// IDs returns the ID of known namespaces
	IDs() []ident.ID

	// Metadatas returns the metadata of known namespaces
	Metadatas() []Metadata
}

// Watch is a watch on a namespace Map
type Watch interface {
	// C is the notification channel for when a value becomes available
	C() <-chan struct{}

	// Get the current namespace map
	Get() Map

	// Close closes the watch
	Close() error
}

// Registry is an un-changing container for a Map
type Registry interface {
	// Watch for the Registry changes
	Watch() (Watch, error)

	// Close closes the registry
	Close() error
}

// Initializer can init new instances of namespace registries
type Initializer interface {
	// Init will return a new Registry
	Init() (Registry, error)
}

// DynamicOptions is a set of options for dynamic namespace registry
type DynamicOptions interface {
	// Validate validates the options
	Validate() error

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(value instrument.Options) DynamicOptions

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetConfigServiceClient sets the client of ConfigService
	SetConfigServiceClient(c client.Client) DynamicOptions

	// ConfigServiceClient returns the client of ConfigService
	ConfigServiceClient() client.Client

	// SetNamespaceRegistryKey sets the kv-store key used for the
	// NamespaceRegistry
	SetNamespaceRegistryKey(k string) DynamicOptions

	// NamespaceRegistryKey returns the kv-store key used for the
	// NamespaceRegistry
	NamespaceRegistryKey() string

	// SetInitTimeout sets the waiting time for dynamic topology to be initialized
	SetInitTimeout(value time.Duration) DynamicOptions

	// InitTimeout returns the waiting time for dynamic topology to be initialized
	InitTimeout() time.Duration
}
