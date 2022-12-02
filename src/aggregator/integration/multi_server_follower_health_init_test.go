//go:build integration
// +build integration

// Copyright (c) 2022 Uber Technologies, Inc.
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

package integration

import (
	"sync"
	"sync/atomic"
	"testing"

	httpserver "github.com/m3db/m3/src/aggregator/server/http"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:dupl
func TestMultiServerFollowerHealthInit(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	servers, _, _ := newTestServerSetups(t)

	// Start the servers.
	log := xtest.NewLogger(t)
	log.Info("test forwarding pipeline")
	for i, server := range servers {
		require.NoError(t, server.startServer())
		log.Sugar().Infof("server %d is now up", i)
	}

	// Stop the servers.
	for i, server := range servers {
		i := i
		server := server
		defer func() {
			require.NoError(t, server.stopServer())
			log.Sugar().Infof("server %d is now down", i)
		}()
	}

	// Waiting for two leaders to come up.
	var (
		leaders    = make(map[int]struct{})
		leaderCh   = make(chan int, len(servers)/2)
		numLeaders int32
		wg         sync.WaitGroup
	)
	wg.Add(len(servers) / 2)
	for i, server := range servers {
		i, server := i, server
		go func() {
			if err := server.waitUntilLeader(); err == nil {
				res := int(atomic.AddInt32(&numLeaders, 1))
				if res <= len(servers)/2 {
					leaderCh <- i
					wg.Done()
				}
			}
		}()
	}
	wg.Wait()
	close(leaderCh)

	for i := range leaderCh {
		leaders[i] = struct{}{}
		log.Sugar().Infof("server %d has become the leader", i)
	}
	log.Sugar().Infof("%d servers have become leaders", len(leaders))

	for _, server := range servers {
		var resp httpserver.StatusResponse
		require.NoError(t, server.getStatusResponse(httpserver.StatusPath, &resp))

		// No data has been written to the aggregators yet, but all servers (including the
		// followers) should be able to lead.
		assert.True(t, resp.Status.FlushStatus.CanLead)
	}
}
