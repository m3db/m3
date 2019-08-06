package client

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/topology"
)

var (
	errInvalidSyncCount = errors.New("must supply exactly one synchronous topology initializer")
)

var _ MultiOptions = (*multiOptions)(nil)

type multiOptions struct {
	options
	topologyInitializers      []topology.Initializer
	asyncTopologyInitializers []topology.Initializer
}

func NewMultiOptions() MultiOptions {
	return &multiOptions{
		options:              *newOptions(),
		topologyInitializers: []topology.Initializer{},
	}
}

func NewAdminMultiOptions() AdminMultiOptions {
	return &multiOptions{
		options:              *newOptions(),
		topologyInitializers: []topology.Initializer{},
	}
}

func (o *multiOptions) SetTopologyInitializers(value []topology.Initializer) Options {
	opts := *o
	opts.topologyInitializers = value
	return &opts
}

func (o *multiOptions) TopologyInitializers() []topology.Initializer {
	return o.topologyInitializers
}

func (o *multiOptions) SetAsyncTopologyInitializers(value []topology.Initializer) Options {
	opts := *o
	opts.asyncTopologyInitializers = value
	return &opts
}

func (o *multiOptions) AsyncTopologyInitializers() []topology.Initializer {
	return o.asyncTopologyInitializers
}

func (o *multiOptions) Validate() error {
	if len(o.topologyInitializers) != 0 {
		return errInvalidSyncCount
	}
	return validate(&o.options)
}
