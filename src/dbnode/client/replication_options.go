// Copyright (c) 2019 Uber Technologies, Inc.
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

package client

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/topology"
)

var (
	errInvalidSyncCount = errors.New("must supply exactly one synchronous topology initializer")
)

var _ ReplicatedOptions = (*replicatedOptions)(nil)

type replicatedOptions struct {
	options                   Options
	asyncTopologyInitializers []topology.Initializer
}

// NewReplicatedOptions creates a new set of multi cluster options
func NewReplicatedOptions() ReplicatedOptions {
	return &replicatedOptions{
		options:                   newOptions(),
		asyncTopologyInitializers: []topology.Initializer{},
	}
}

// NewAdminReplicatedOptions creates a new set of administration multi cluster options
func NewAdminReplicatedOptions() AdminReplicatedOptions {
	return &replicatedOptions{
		options:                   newOptions(),
		asyncTopologyInitializers: []topology.Initializer{},
	}
}

func (o *replicatedOptions) SetOptions(value Options) ReplicatedOptions {
	opts := *o
	opts.options = value
	return &opts
}

func (o *replicatedOptions) Options() Options {
	return o.options
}

func (o *replicatedOptions) SetAdminOptions(value AdminOptions) AdminReplicatedOptions {
	opts := *o
	opts.options = value
	return &opts
}

func (o *replicatedOptions) AdminOptions() AdminOptions {
	return o.options.(AdminOptions)
}

func (o *replicatedOptions) SetAsyncTopologyInitializers(value []topology.Initializer) ReplicatedOptions {
	opts := *o
	opts.asyncTopologyInitializers = value
	return &opts
}

func (o *replicatedOptions) AsyncTopologyInitializers() []topology.Initializer {
	return o.asyncTopologyInitializers
}

func (o *replicatedOptions) OptionsForAsyncClusters() []Options {
	result := make([]Options, 0, len(o.asyncTopologyInitializers))
	for _, topoInit := range o.asyncTopologyInitializers {
		options := o.Options().SetTopologyInitializer(topoInit)
		result = append(result, options)
	}
	return result
}
