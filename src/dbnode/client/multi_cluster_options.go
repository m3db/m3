package client

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/topology"
)

var (
	errInvalidSyncCount = errors.New("must supply exactly one synchronous topology initializer")
)

var _ MultiClusterOptions = (*multiClusterOptions)(nil)

type multiClusterOptions struct {
	options
	asyncTopologyInitializers []topology.Initializer
}

// NewMultiClusterOptions creates a new set of multi cluster options
func NewMultiClusterOptions() MultiClusterOptions {
	return &multiClusterOptions{
		options:                   *newOptions(),
		asyncTopologyInitializers: []topology.Initializer{},
	}
}

// NewAdminMultiClusterOptions creates a new set of administration multi cluster options
func NewAdminMultiClusterOptions() AdminMultiClusterOptions {
	return &multiClusterOptions{
		options:                   *newOptions(),
		asyncTopologyInitializers: []topology.Initializer{},
	}
}

func (o *multiClusterOptions) SetAsyncTopologyInitializers(value []topology.Initializer) MultiClusterOptions {
	opts := *o
	opts.asyncTopologyInitializers = value
	return &opts
}

func (o *multiClusterOptions) AsyncTopologyInitializers() []topology.Initializer {
	return o.asyncTopologyInitializers
}

func (o *multiClusterOptions) OptionsForAsyncClusters() []Options {
	result := make([]Options, 0, len(o.asyncTopologyInitializers))
	for _, topoInit := range o.asyncTopologyInitializers {
		result = append(result, o.SetTopologyInitializer(topoInit))
	}
	return result
}
