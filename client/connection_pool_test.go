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

package client

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3db/topology"
	xclose "github.com/m3db/m3db/x/close"

	"github.com/stretchr/testify/assert"
)

var (
	h           = topology.NewHost("testhost:9000")
	channelNone = &nullChannel{}
)

type nullChannel struct{}

func (*nullChannel) Close() {}

func newConnectionPoolTestOptions() m3db.ClientOptions {
	return NewOptions().
		BackgroundConnectInterval(5 * time.Millisecond).
		BackgroundConnectStutter(2 * time.Millisecond).
		BackgroundHealthCheckInterval(5 * time.Millisecond).
		BackgroundHealthCheckStutter(2 * time.Millisecond)
}

func TestConnectionPoolConnectsAndRetriesConnects(t *testing.T) {
	// Scenario:
	// 1. Try fill 4 connections
	// > Fail 1 on connection step, have 3 connections
	// 2. Try fill remaining connection
	// > Fail 1 on health check, have 3 connections
	// 3. Try fill remaining connection
	// > Fulfill remaining connection, have 4 connections
	// 4. Don't bother

	var (
		attempts             int
		sleeps               int
		rounds               int32
		firstSleepWg         sync.WaitGroup
		proceedFirstSleepWg  sync.WaitGroup
		secondSleepWg        sync.WaitGroup
		proceedSecondSleepWg sync.WaitGroup
		thirdSleepWg         sync.WaitGroup
		proceedThirdSleepWg  sync.WaitGroup
		fourthSleepWg        sync.WaitGroup
		doneWg               sync.WaitGroup
	)
	firstSleepWg.Add(1)
	proceedFirstSleepWg.Add(1)
	secondSleepWg.Add(1)
	proceedSecondSleepWg.Add(1)
	thirdSleepWg.Add(1)
	proceedThirdSleepWg.Add(1)
	fourthSleepWg.Add(1)
	doneWg.Add(1)

	opts := newConnectionPoolTestOptions()
	opts = opts.MaxConnectionCount(4)
	conns := newConnectionPool(h, opts).(*connPool)
	conns.newConn = func(ch string, addr string, opts m3db.ClientOptions) (xclose.SimpleCloser, rpc.TChanNode, error) {
		attempts++
		if attempts == 1 {
			return nil, nil, fmt.Errorf("a connect error")
		}
		return channelNone, nil, nil
	}
	conns.healthCheckNewConn = func(client rpc.TChanNode, opts m3db.ClientOptions) error {
		if atomic.LoadInt32(&rounds) == 1 {
			// If second round then fail health check
			return fmt.Errorf("a health check error")
		}
		return nil
	}
	conns.healthCheck = func(client rpc.TChanNode, opts m3db.ClientOptions) error {
		return nil
	}
	conns.sleepConnect = func(t time.Duration) {
		sleeps++
		if sleeps == 1 {
			firstSleepWg.Done()
			proceedFirstSleepWg.Wait()
		} else if sleeps == 2 {
			secondSleepWg.Done()
			proceedSecondSleepWg.Wait()
		} else if sleeps == 3 {
			thirdSleepWg.Done()
			proceedThirdSleepWg.Wait()
		} else if sleeps == 4 {
			fourthSleepWg.Done()
			doneWg.Wait()
			return // All done
		}
		atomic.AddInt32(&rounds, 1)
		time.Sleep(time.Millisecond)
	}

	assert.Equal(t, 0, conns.GetConnectionCount())

	conns.Open()

	// Wait for first round, should've created all conns except first
	firstSleepWg.Wait()
	assert.Equal(t, 3, conns.GetConnectionCount())
	proceedFirstSleepWg.Done()

	// Wait for second round, all attempts should succeed but all fail health checks
	secondSleepWg.Wait()
	assert.Equal(t, 3, conns.GetConnectionCount())
	proceedSecondSleepWg.Done()

	// Wait for third round, now should succeed and all connections accounted for
	thirdSleepWg.Wait()
	assert.Equal(t, 4, conns.GetConnectionCount())
	doneAll := attempts
	proceedThirdSleepWg.Done()

	// Wait for fourth roundm, now should not involve attempting to spawn connections
	fourthSleepWg.Wait()
	// Ensure no more attempts done in fnal round
	assert.Equal(t, doneAll, attempts)

	conns.Close()
	doneWg.Done()
}
