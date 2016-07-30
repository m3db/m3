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
	"os"
	"sync"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
)

// fileSystemClient implements the Client interface by polling the local filesystem
// for topology changes.
type fileSystemClient struct {
	sync.Mutex

	fd           *os.File                             // path to the file containing topology information
	pollInterval time.Duration                        // polling frequency
	subscribers  map[m3db.TopologySubscriber]struct{} // list of subscribers

	wg       sync.WaitGroup // wait group for goroutine to exit
	exitChan chan struct{}  // exit channel
}

// NewFileSystemClient creates a new filesystem-based topology client.
func NewFileSystemClient(path string, pollInterval time.Duration) (m3db.TopologyClient, error) {
	// TODO(xichen): create a file descriptor and spin up a goroutine that polls the fd
	// every pollInterval and broadcast changes to all subscribers. polling goroutine should
	// call wg.Done() and checks the exit channel every now and then.
	return nil, nil
}

func (c *fileSystemClient) AddSubscriber(subscriber m3db.TopologySubscriber) m3db.TopologyMap {
	c.Lock()
	c.subscribers[subscriber] = struct{}{}
	c.Unlock()
	return nil
}

func (c *fileSystemClient) RemoveSubscriber(subscriber m3db.TopologySubscriber) {
	c.Lock()
	delete(c.subscribers, subscriber)
	c.Unlock()
}

func (c *fileSystemClient) Close() error {
	c.exitChan <- struct{}{}
	c.wg.Wait()
	return nil
}
