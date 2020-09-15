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

// NewState creates state specifically to be used during the bootstrap process.
// Primarily a mechanism for passing info files along without needing to re-read them at each
// stage of the bootstrap process.
func NewState(options StateOptions) (State, error) {
	if err := options.Validate(); err != nil {
		return State{}, err
	}
	return State{
		fsOpts:           options.FilesystemOptions(),
		infoFilesFinders: options.InfoFilesFinders(),
		iOpts:            options.InstrumentOptions(),
	}, nil
}

// InfoFilesForNamespace returns the info files grouped by shard for the provided namespace.
func (r *State) InfoFilesForNamespace(ns namespace.Metadata) InfoFileResultsPerShard {
	infoFilesByShard, ok := r.ReadInfoFiles()[ns]
	// This should never happen as State object is initialized with all namespaces to bootstrap.
	if !ok {
		instrument.EmitAndLogInvariantViolation(r.iOpts, func(l *zap.Logger) {
			l.Error("attempting to read info files for namespace not specified at bootstrap startup",
				zap.String("namespace", ns.ID().String()))
		})
		return make(InfoFileResultsPerShard)
	}
	return infoFilesByShard
}

// InfoFilesForShard returns the info files grouped by shard for the provided namespace.
func (r *State) InfoFilesForShard(ns namespace.Metadata, shard uint32) []fs.ReadInfoFileResult {
	infoFileResults, ok := r.InfoFilesForNamespace(ns)[shard]
	// This should never happen as State object is initialized with all shards to bootstrap.
	if !ok {
		instrument.EmitAndLogInvariantViolation(r.iOpts, func(l *zap.Logger) {
			l.Error("attempting to read info files for shard not specified at bootstrap startup",
				zap.String("namespace", ns.ID().String()), zap.Uint32("shard", shard))

		})
		return make([]fs.ReadInfoFileResult, 0)
	}
	return infoFileResults
}

// TODO(nate): Make this threadsafe? If so, we'll need to clone the map
// before returning, provide an update method, and incorporate locking.
//
// ReadInfoFiles returns info file results for each shard grouped by namespace. A cached copy
// is returned if the info files have already been read.
func (r *State) ReadInfoFiles() InfoFilesByNamespace {
	if r.infoFilesByNamespace != nil {
		return r.infoFilesByNamespace
	}

	r.infoFilesByNamespace = make(InfoFilesByNamespace, len(r.infoFilesFinders))
	for _, finder := range r.infoFilesFinders {
		result := make(InfoFileResultsPerShard, len(finder.Shards))
		for _, shard := range finder.Shards {
			result[shard] = fs.ReadInfoFiles(r.fsOpts.FilePathPrefix(),
				finder.Namespace.ID(), shard, r.fsOpts.InfoReaderBufferSize(), r.fsOpts.DecodingOptions(),
				persist.FileSetFlushType)
		}

		r.infoFilesByNamespace[finder.Namespace] = result
	}

	return r.infoFilesByNamespace
}

type stateOptions struct {
	fsOpts           fs.Options
	infoFilesFinders []InfoFilesFinder
	iOpts            instrument.Options
}

// NewStateOptions creates new StateOptions.
func NewStateOptions() StateOptions {
	return &stateOptions{}
}

func (s *stateOptions) Validate() error {
	if s.fsOpts == nil {
		return errFilesystemOptsNotSet
	}
	if err := s.fsOpts.Validate(); err != nil {
		return err
	}
	if s.iOpts == nil {
		return errInstrumentOptsNotSet
	}
	return nil
}

func (s *stateOptions) SetFilesystemOptions(value fs.Options) StateOptions {
	opts := *s
	opts.fsOpts = value
	return &opts
}

func (s *stateOptions) FilesystemOptions() fs.Options {
	return s.fsOpts
}

func (s *stateOptions) SetInfoFilesFinders(value []InfoFilesFinder) StateOptions {
	opts := *s
	opts.infoFilesFinders = value
	return &opts
}

func (s *stateOptions) InfoFilesFinders() []InfoFilesFinder {
	return s.infoFilesFinders
}

func (s *stateOptions) SetInstrumentOptions(value instrument.Options) StateOptions {
	opts := *s
	opts.iOpts = value
	return &opts
}

func (s *stateOptions) InstrumentOptions() instrument.Options {
	return s.iOpts
}
