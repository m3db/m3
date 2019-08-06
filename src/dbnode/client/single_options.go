package client

import "github.com/m3db/m3/src/dbnode/topology"

var _ SingleOptions = (*singleOptions)(nil)

type singleOptions struct {
	options
	topologyInitializer topology.Initializer
}

func NewSingleOptions() SingleOptions {
	return &singleOptions{
		options: *newOptions(),
	}
}

func NewAdminSingleOptions() AdminSingleOptions {
	return &singleOptions{
		options: *newOptions(),
	}
}

func (o *singleOptions) SetTopologyInitializer(value topology.Initializer) Options {
	opts := *o
	opts.topologyInitializer = value
	return &opts
}

func (o *singleOptions) TopologyInitializer() topology.Initializer {
	return o.topologyInitializer
}

func (o *singleOptions) Validate() error {
	if o.topologyInitializer == nil {
		return errNoTopologyInitializerSet
	}
	return validate(&o.options)
}
