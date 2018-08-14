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

package util

import (
	"io"
	"time"

	m3emnode "github.com/m3db/m3/src/dbnode/x/m3em/node"
)

// NodePredicate is a predicate on a M3DB ServiceNode
type NodePredicate func(m3emnode.Node) bool

// NodesWatcher makes it easy to monitor observable properties
// of M3DB ServiceNodes
type NodesWatcher interface {
	io.Closer

	// WaitUntilAll allows you to specify a predicate which must be satisfied
	// on all monitored Nodes within the timeout provided. It returns a flag
	// indicating if this occurred successfully
	WaitUntilAll(p NodePredicate, timeout time.Duration) bool

	// Pending returns the list of nodes which have not satisfied the
	// predicate satisfied
	Pending() []m3emnode.Node

	// PendingAsError returns the list of pending nodes wrapped as an
	// error
	PendingAsError() error
}
