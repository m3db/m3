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
