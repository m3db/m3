// Copyright (c) 2017 Uber Technologies, Inc.
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

package m3db

import (
	"github.com/m3db/m3em/node"
	"github.com/m3db/m3x/instrument"
)

// Node represents a ServiceNode pointing to an M3DB process
type Node interface {
	node.ServiceNode

	// Health returns the health for this node
	Health() (NodeHealth, error)

	// Bootstrapped returns whether the node is bootstrapped
	Bootstrapped() bool
}

// NodeHealth provides Health information for a M3DB node
type NodeHealth struct {
	Bootstrapped bool
	Status       string
	OK           bool
}

// Options represent the knobs to control m3db.Node behavior
type Options interface {
	// Validate validates the Options
	Validate() error

	// SetInstrumentOptions sets the instrumentation options
	SetInstrumentOptions(instrument.Options) Options

	// InstrumentOptions returns the instrumentation options
	InstrumentOptions() instrument.Options

	// SetNodeOptions sets the node options
	SetNodeOptions(node.Options) Options

	// NodeOptions returns the node options
	NodeOptions() node.Options
}
