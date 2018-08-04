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
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	m3emnode "github.com/m3db/m3/src/dbnode/x/m3em/node"
	xclock "github.com/m3db/m3x/clock"
	xlog "github.com/m3db/m3x/log"
)

type nodesWatcher struct {
	sync.Mutex
	pending           map[string]m3emnode.Node
	logger            xlog.Logger
	reportingInterval time.Duration
}

// NewNodesWatcher creates a new NodeWatcher
func NewNodesWatcher(
	nodes []m3emnode.Node,
	logger xlog.Logger,
	reportingInterval time.Duration,
) NodesWatcher {
	watcher := &nodesWatcher{
		pending:           make(map[string]m3emnode.Node, len(nodes)),
		logger:            logger,
		reportingInterval: reportingInterval,
	}
	for _, node := range nodes {
		watcher.addInstanceWithLock(node)
	}
	return watcher
}

func (nw *nodesWatcher) addInstanceWithLock(node m3emnode.Node) {
	nw.pending[node.ID()] = node
}

func (nw *nodesWatcher) removeInstance(id string) {
	nw.Lock()
	defer nw.Unlock()
	delete(nw.pending, id)
}

func (nw *nodesWatcher) Close() error {
	return nil
}

func (nw *nodesWatcher) Pending() []m3emnode.Node {
	nw.Lock()
	defer nw.Unlock()

	pending := make([]m3emnode.Node, 0, len(nw.pending))
	for _, node := range nw.pending {
		pending = append(pending, node)
	}

	sort.Sort(nodesByID(pending))
	return pending
}

func (nw *nodesWatcher) pendingStatus() (int, string) {
	nw.Lock()
	defer nw.Unlock()

	numPending := 0
	var buffer bytes.Buffer
	for _, node := range nw.pending {
		numPending++
		if numPending == 1 {
			buffer.WriteString(node.ID())
		} else {
			buffer.WriteString(fmt.Sprintf(", %s", node.ID()))
		}
	}

	return numPending, buffer.String()
}

func (nw *nodesWatcher) PendingAsError() error {
	numPending, pendingString := nw.pendingStatus()
	if numPending == 0 {
		return nil
	}
	return fmt.Errorf("%d nodes not bootstrapped: %s", numPending, pendingString)
}

func (nw *nodesWatcher) WaitUntilAll(p NodePredicate, timeout time.Duration) bool {
	// kick of go-routines to check condition for each pending node
	pending := nw.Pending()
	var wg sync.WaitGroup
	wg.Add(len(pending))
	for i := range pending {
		n := pending[i]
		go func(node m3emnode.Node) {
			defer wg.Done()
			if cond := xclock.WaitUntil(func() bool { return p(node) }, timeout); cond {
				nw.logger.Infof("%s finished bootstrapping", node.ID())
				nw.removeInstance(node.ID())
			}
		}(n)
	}

	// kick of a go-routine to log information
	doneCh := make(chan struct{})
	closeCh := make(chan struct{})
	reporter := time.NewTicker(nw.reportingInterval)
	defer func() {
		reporter.Stop()
		close(closeCh)
		close(doneCh)
	}()
	go func() {
		for {
			select {
			case <-closeCh:
				doneCh <- struct{}{}
				return
			case <-reporter.C:
				numPending, pendingString := nw.pendingStatus()
				nw.logger.Infof("%d instances remaining: [%v]", numPending, pendingString)
				if numPending == 0 {
					doneCh <- struct{}{}
					return
				}
			}
		}
	}()

	// wait until all nodes complete or timeout
	wg.Wait()

	// cleanup
	closeCh <- struct{}{}
	<-doneCh

	// report if all nodes completed
	numPending, _ := nw.pendingStatus()
	return numPending == 0
}

type nodesByID []m3emnode.Node

func (n nodesByID) Len() int {
	return len(n)
}

func (n nodesByID) Less(i, j int) bool {
	return n[i].ID() < n[j].ID()
}

func (n nodesByID) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}
