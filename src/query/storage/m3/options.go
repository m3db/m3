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

package m3

import (
	"github.com/m3db/m3/src/x/instrument"
)

type dynamicClusterOptions struct {
	config                   []DynamicClusterNamespaceConfiguration
	iOpts                    instrument.Options
	clusterNamespacesWatcher ClusterNamespacesWatcher
}

// NewDynamicClusterOptions returns new DynamicClusterOptions.
func NewDynamicClusterOptions() DynamicClusterOptions {
	return &dynamicClusterOptions{
		iOpts: instrument.NewOptions(),
	}
}

func (d *dynamicClusterOptions) Validate() error {
	if len(d.config) == 0 {
		return errDynamicClusterNamespaceConfigurationNotSet
	}
	if d.iOpts == nil {
		return errInstrumentOptionsNotSet
	}
	if d.clusterNamespacesWatcher == nil {
		return errClusterNamespacesWatcherNotSet
	}

	return nil
}

func (d *dynamicClusterOptions) SetDynamicClusterNamespaceConfiguration(
	value []DynamicClusterNamespaceConfiguration,
) DynamicClusterOptions {
	opts := *d
	opts.config = value
	return &opts
}

func (d *dynamicClusterOptions) DynamicClusterNamespaceConfiguration() []DynamicClusterNamespaceConfiguration {
	return d.config
}

func (d *dynamicClusterOptions) SetInstrumentOptions(value instrument.Options) DynamicClusterOptions {
	opts := *d
	opts.iOpts = value
	return &opts
}

func (d *dynamicClusterOptions) InstrumentOptions() instrument.Options {
	return d.iOpts
}

func (d *dynamicClusterOptions) SetClusterNamespacesWatcher(value ClusterNamespacesWatcher) DynamicClusterOptions {
	opts := *d
	opts.clusterNamespacesWatcher = value
	return &opts
}

func (d *dynamicClusterOptions) ClusterNamespacesWatcher() ClusterNamespacesWatcher {
	return d.clusterNamespacesWatcher
}
