// Copyright (c) 2021  Uber Technologies, Inc.
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

package inprocess

import (
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/x/errors"
)

type inprocessM3Resources struct {
	coordinator resources.Coordinator
	dbNodes     resources.Nodes
	aggregators resources.Aggregators
}

// ResourceOptions are the options for creating new
// resources.M3Resources.
type ResourceOptions struct {
	Coordinator resources.Coordinator
	DBNodes     resources.Nodes
	Aggregators resources.Aggregators
}

// NewM3Resources returns an implementation of resources.M3Resources
// backed by in-process implementations of the M3 components.
func NewM3Resources(options ResourceOptions) resources.M3Resources {
	return &inprocessM3Resources{
		coordinator: options.Coordinator,
		dbNodes:     options.DBNodes,
		aggregators: options.Aggregators,
	}
}

func (i *inprocessM3Resources) Start() {
	for _, node := range i.dbNodes {
		node.Start()
	}
	for _, agg := range i.aggregators {
		agg.Start()
	}
	i.coordinator.Start()
}

func (i *inprocessM3Resources) Cleanup() error {
	err := errors.NewMultiError()
	if i.coordinator != nil {
		err = err.Add(i.coordinator.Close())
	}

	for _, d := range i.dbNodes {
		err = err.Add(d.Close())
	}

	for _, a := range i.aggregators {
		err = err.Add(a.Close())
	}

	return err.FinalError()
}

func (i *inprocessM3Resources) Nodes() resources.Nodes {
	return i.dbNodes
}

func (i *inprocessM3Resources) Coordinator() resources.Coordinator {
	return i.coordinator
}

func (i *inprocessM3Resources) Aggregators() resources.Aggregators {
	return i.aggregators
}
