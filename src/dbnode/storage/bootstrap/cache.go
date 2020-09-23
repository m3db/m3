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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

var (
	errFilesystemOptsNotSet = errors.New("filesystemOptions not set")
	errInstrumentOptsNotSet = errors.New("instrumentOptions not set")
)

// NewCache creates a cache specifically to be used during the bootstrap process.
// Primarily a mechanism for passing info files along without needing to re-read them at each
// stage of the bootstrap process.
func NewCache(options CacheOptions) (Cache, error) {
	if err := options.Validate(); err != nil {
		return Cache{}, err
	}
	return Cache{
		fsOpts:           options.FilesystemOptions(),
		infoFilesFinders: options.InfoFilesFinders(),
		iOpts:            options.InstrumentOptions(),
	}, nil
}

// InfoFilesForNamespace returns the info files grouped by shard for the provided namespace.
func (c *Cache) InfoFilesForNamespace(ns namespace.Metadata) InfoFileResultsPerShard {
	infoFilesByShard, ok := c.ReadInfoFiles()[ns]
	// This should never happen as Cache object is initialized with all namespaces to bootstrap.
	if !ok {
		instrument.EmitAndLogInvariantViolation(c.iOpts, func(l *zap.Logger) {
			l.Error("attempting to read info files for namespace not specified at bootstrap startup",
				zap.String("namespace", ns.ID().String()))
		})
		return make(InfoFileResultsPerShard)
	}
	return infoFilesByShard
}

// InfoFilesForShard returns the info files grouped by shard for the provided namespace.
func (c *Cache) InfoFilesForShard(ns namespace.Metadata, shard uint32) []fs.ReadInfoFileResult {
	infoFileResults, ok := c.InfoFilesForNamespace(ns)[shard]
	// This should never happen as Cache object is initialized with all shards to bootstrap.
	if !ok {
		instrument.EmitAndLogInvariantViolation(c.iOpts, func(l *zap.Logger) {
			l.Error("attempting to read info files for shard not specified at bootstrap startup",
				zap.String("namespace", ns.ID().String()), zap.Uint32("shard", shard))

		})
		return make([]fs.ReadInfoFileResult, 0)
	}
	return infoFileResults
}

// ReadInfoFiles returns info file results for each shard grouped by namespace. A cached copy
// is returned if the info files have already been read.
func (c *Cache) ReadInfoFiles() InfoFilesByNamespace {
	if c.infoFilesByNamespace != nil {
		return c.infoFilesByNamespace
	}

	c.infoFilesByNamespace = make(InfoFilesByNamespace, len(c.infoFilesFinders))
	for _, finder := range c.infoFilesFinders {
		result := make(InfoFileResultsPerShard, len(finder.Shards))
		for _, shard := range finder.Shards {
			result[shard] = fs.ReadInfoFiles(c.fsOpts.FilePathPrefix(),
				finder.Namespace.ID(), shard, c.fsOpts.InfoReaderBufferSize(), c.fsOpts.DecodingOptions(),
				persist.FileSetFlushType)
		}

		c.infoFilesByNamespace[finder.Namespace] = result
	}

	return c.infoFilesByNamespace
}

type cacheOptions struct {
	fsOpts           fs.Options
	infoFilesFinders []InfoFilesFinder
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

func (c *cacheOptions) SetInfoFilesFinders(value []InfoFilesFinder) CacheOptions {
	opts := *c
	opts.infoFilesFinders = value
	return &opts
}

func (c *cacheOptions) InfoFilesFinders() []InfoFilesFinder {
	return c.infoFilesFinders
}

func (c *cacheOptions) SetInstrumentOptions(value instrument.Options) CacheOptions {
	opts := *c
	opts.iOpts = value
	return &opts
}

func (c *cacheOptions) InstrumentOptions() instrument.Options {
	return c.iOpts
}
