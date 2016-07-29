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

package topology

import (
	"github.com/m3db/m3db/interfaces/m3db"
)

type dynamicTopologyType struct {
	opts m3db.DynamicTopologyTypeOptions
}

// NewDynamicTopologyType creates a new dynamic topology type
func NewDynamicTopologyType(opts m3db.DynamicTopologyTypeOptions) m3db.TopologyType {
	return &dynamicTopologyType{opts: opts}
}

func (t *dynamicTopologyType) Create() (m3db.Topology, error) {
	if err := t.opts.Validate(); err != nil {
		return nil, err
	}
	return newDynamicTopology(t.opts), nil
}

func (t *dynamicTopologyType) Options() m3db.TopologyTypeOptions {
	return t.opts
}

type dynamicTopology struct {
	client m3db.TopologyClient
}

// newDynamicTopology creates a new dynamic topology.
func newDynamicTopology(opts m3db.DynamicTopologyTypeOptions) m3db.Topology {
	newClientFn := opts.GetNewTopologyClientFn()
	return &dynamicTopology{client: newClientFn()}
}

func (t *dynamicTopology) GetAndSubscribe(subscriber m3db.TopologySubscriber) m3db.TopologyMap {
	return t.client.AddSubscriber(subscriber)
}

func (t *dynamicTopology) PostUpdate(update m3db.TopologyUpdate) {
	t.client.PostUpdate(update)
}

func (t *dynamicTopology) Close() error {
	return t.client.Close()
}
