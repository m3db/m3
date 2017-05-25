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
	"fmt"
	"sync"
	"time"

	m3dbnode "github.com/m3db/m3db/x/m3em/node"

	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/errors"
)

type nodesWatcher struct {
	sync.Mutex
	pending map[string]m3dbnode.Node
}

// NewM3DBNodesWatcher creates a new M3DBNodeWatcher
func NewM3DBNodesWatcher(nodes []m3dbnode.Node) M3DBNodesWatcher {
	watcher := &nodesWatcher{
		pending: make(map[string]m3dbnode.Node, len(nodes)),
	}
	for _, node := range nodes {
		watcher.addInstance(node)
	}
	return watcher
}

func (nw *nodesWatcher) addInstance(node m3dbnode.Node) {
	nw.Lock()
	nw.pending[node.ID()] = node
	nw.Unlock()
}

func (nw *nodesWatcher) removeInstanceWithLock(id string) {
	delete(nw.pending, id)
}

func (nw *nodesWatcher) Pending() []m3dbnode.Node {
	nw.Lock()
	defer nw.Unlock()

	pending := make([]m3dbnode.Node, 0, len(nw.pending))
	for _, node := range nw.pending {
		pending = append(pending, node)
	}

	return pending
}

func (nw *nodesWatcher) PendingAsError() error {
	nw.Lock()
	defer nw.Unlock()
	var multiErr xerrors.MultiError
	for _, node := range nw.pending {
		multiErr = multiErr.Add(fmt.Errorf("node not bootstrapped: %v", node.ID()))
	}
	return multiErr.FinalError()
}

func (nw *nodesWatcher) WaitUntilAll(p M3DBNodePredicate, timeout time.Duration) bool {
	nw.Lock()
	defer nw.Unlock()
	var wg sync.WaitGroup
	for id := range nw.pending {
		wg.Add(1)
		m3dbNode := nw.pending[id]
		go func(m3dbNode m3dbnode.Node) {
			defer wg.Done()
			if cond := xclock.WaitUntil(func() bool { return p(m3dbNode) }, timeout); cond {
				nw.removeInstanceWithLock(m3dbNode.ID())
			}
		}(m3dbNode)
	}
	wg.Wait()
	return len(nw.pending) == 0
}

func (nw *nodesWatcher) WaitUntilAny(p M3DBNodePredicate, timeout time.Duration) bool {
	nw.Lock()
	defer nw.Unlock()
	anyCh := make(chan struct{})
	for id := range nw.pending {
		m3dbNode := nw.pending[id]
		go func(m3dbNode m3dbnode.Node) {
			if cond := xclock.WaitUntil(func() bool { return p(m3dbNode) }, timeout); cond {
				nw.removeInstanceWithLock(m3dbNode.ID())
				anyCh <- struct{}{}
			}
		}(m3dbNode)
	}
	select {
	case <-anyCh:
		return true

	case <-time.After(timeout):
		return false
	}
}
