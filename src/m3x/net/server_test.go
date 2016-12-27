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

package xnet

import (
	"fmt"
	"io/ioutil"
	"net"
	"sort"
	"sync"
	"testing"

	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/assert"
)

func TestStartAcceptLoop(t *testing.T) {
	var (
		results        []string
		resultLock     sync.Mutex
		wgClient       sync.WaitGroup
		wgServer       sync.WaitGroup
		numConnections = 10
		retrier        = xretry.NewRetrier(xretry.NewOptions())
	)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)
	connCh, errCh := StartAcceptLoop(l, retrier)

	wgServer.Add(1)
	go func() {
		defer wgServer.Done()

		curr := 0
		for conn := range connCh {
			fmt.Fprintf(conn, "%d", curr)
			conn.Close()
			curr++
		}
	}()

	for i := 0; i < numConnections; i++ {
		wgClient.Add(1)
		go func() {
			defer wgClient.Done()

			conn, err := net.Dial("tcp", l.Addr().String())
			assert.Nil(t, err)

			data, err := ioutil.ReadAll(conn)
			assert.Nil(t, err)

			resultLock.Lock()
			results = append(results, string(data))
			resultLock.Unlock()
		}()
	}

	wgClient.Wait()

	// Close the listener to intentionally cause a server-side connection error
	l.Close()
	wgServer.Wait()

	sort.Strings(results)

	var expected []string
	for i := 0; i < numConnections; i++ {
		expected = append(expected, fmt.Sprintf("%d", i))
	}
	assert.Equal(t, expected, results)

	// Expect a server-side connection error
	err = <-errCh
	assert.NotNil(t, err)
}
