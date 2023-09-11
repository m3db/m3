// Copyright (c) 2020  Uber Technologies, Inc.
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

package bootstrap

import (
	"errors"
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	errFilesystemOptsNotSet = errors.New("filesystemOptions not set")
	errInstrumentOptsNotSet = errors.New("instrumentOptions not set")
)

type cache struct {
	sync.Mutex

	fsOpts               fs.Options
	namespaceDetails     []NamespaceDetails
	infoFilesByNamespace InfoFilesByNamespace
	iOpts                instrument.Options
	hasPopulatedInfo     bool
}

// NewCache creates a cache specifically to be used during the bootstrap process.
// Primarily a mechanism for passing info files along without needing to re-read them at each
// stage of the bootstrap process.
func NewCache(options CacheOptions) (Cache, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return &cache{
		fsOpts:               options.FilesystemOptions(),
		namespaceDetails:     options.NamespaceDetails(),
		infoFilesByNamespace: make(InfoFilesByNamespace, len(options.NamespaceDetails())),
		iOpts:                options.InstrumentOptions(),
	}, nil
}

func (c *cache) InfoFilesForNamespace(ns namespace.Metadata) (InfoFileResultsPerShard, error) {
	infoFilesByShard, ok := c.ReadInfoFiles()[ns]
	// This should never happen as Cache object is initialized with all namespaces to bootstrap.
	if !ok {
		return nil, fmt.Errorf("attempting to read info files for namespace %v not specified at bootstrap "+
			"startup", ns.ID().String())
	}
	return infoFilesByShard, nil
}

func (c *cache) InfoFilesForShard(ns namespace.Metadata, shard uint32) ([]fs.ReadInfoFileResult, error) {
	infoFilesByShard, err := c.InfoFilesForNamespace(ns)
	if err != nil {
		return nil, err
	}
	infoFileResults, ok := infoFilesByShard[shard]
	// This should never happen as Cache object is initialized with all shards to bootstrap.
	if !ok {
		return nil, fmt.Errorf("attempting to read info files for shard %v not specified "+
			"at bootstrap startup for namespace %v", shard, ns.ID().String())
	}
	return infoFileResults, nil
}

func (c *cache) Evict() {
	c.Lock()
	defer c.Unlock()
	c.hasPopulatedInfo = false
}

func (c *cache) ReadInfoFiles() InfoFilesByNamespace {
	c.Lock()
	defer c.Unlock()
	if !c.hasPopulatedInfo {
		c.populateInfoFilesByNamespaceWithLock()
		c.hasPopulatedInfo = true
	}
	return c.infoFilesByNamespace
}

func (c *cache) populateInfoFilesByNamespaceWithLock() {
	for _, finder := range c.namespaceDetails {
		// NB(bodu): It is okay to reuse the info files by ns results per shard here
		// as the shards were set in the cache ctor and do not change per invocation.
		result, ok := c.infoFilesByNamespace[finder.Namespace]
		if !ok {
			result = make(InfoFileResultsPerShard, len(finder.Shards))
		}
		for _, shard := range finder.Shards {
			result[shard] = fs.ReadInfoFiles(c.fsOpts.FilePathPrefix(),
				finder.Namespace.ID(), shard, c.fsOpts.InfoReaderBufferSize(), c.fsOpts.DecodingOptions(),
				persist.FileSetFlushType)
		}

		c.infoFilesByNamespace[finder.Namespace] = result
	}
}

type cacheOptions struct {
	fsOpts           fs.Options
	namespaceDetails []NamespaceDetails
	iOpts            instrument.Options
}

// NewCacheOptions creates new CacheOptions.
func NewCacheOptions() CacheOptions {
	return &cacheOptions{}
}

func (c *cacheOptions) Validate() error {
	if c.fsOpts == nil {
		return errFilesystemOptsNotSet
	}
	if err := c.fsOpts.Validate(); err != nil {
		return err
	}
	if c.iOpts == nil {
		return errInstrumentOptsNotSet
	}
	return nil
}

func (c *cacheOptions) SetFilesystemOptions(value fs.Options) CacheOptions {
	opts := *c
	opts.fsOpts = value
	return &opts
}

func (c *cacheOptions) FilesystemOptions() fs.Options {
	return c.fsOpts
}

func (c *cacheOptions) SetNamespaceDetails(value []NamespaceDetails) CacheOptions {
	opts := *c
	opts.namespaceDetails = value
	return &opts
}

func (c *cacheOptions) NamespaceDetails() []NamespaceDetails {
	return c.namespaceDetails
}

func (c *cacheOptions) SetInstrumentOptions(value instrument.Options) CacheOptions {
	opts := *c
	opts.iOpts = value
	return &opts
}

func (c *cacheOptions) InstrumentOptions() instrument.Options {
	return c.iOpts
}
