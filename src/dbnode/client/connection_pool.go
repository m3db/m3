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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/src/dbnode/generated/thrift/rpc"
	nchannel "github.com/m3db/m3db/src/dbnode/network/server/tchannelthrift/node/channel"
	"github.com/m3db/m3db/src/dbnode/topology"
	xclose "github.com/m3db/m3x/close"

	"github.com/spaolacci/murmur3"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const (
	channelName = "Client"
)

var (
	errConnectionPoolClosed           = errors.New("connection pool closed")
	errConnectionPoolHasNoConnections = errors.New("connection pool has no connections")
)

type connPool struct {
	sync.RWMutex

	opts               Options
	host               topology.Host
	pool               []conn
	poolLen            int64
	used               int64
	connectRand        rand.Source
	healthCheckRand    rand.Source
	newConn            newConnFn
	healthCheckNewConn healthCheckFn
	healthCheck        healthCheckFn
	sleepConnect       sleepFn
	sleepHealth        sleepFn
	sleepHealthRetry   sleepFn
	status             status
}

type conn struct {
	channel xclose.SimpleCloser
	client  rpc.TChanNode
}

type newConnFn func(channelName string, addr string, opts Options) (xclose.SimpleCloser, rpc.TChanNode, error)

type healthCheckFn func(client rpc.TChanNode, opts Options) error

type sleepFn func(t time.Duration)

// Allow for test override
var globalNewConn = newConn

func newConnectionPool(host topology.Host, opts Options) connectionPool {
	seed := int64(murmur3.Sum32([]byte(host.Address())))

	p := &connPool{
		opts:               opts,
		host:               host,
		pool:               make([]conn, 0, opts.MaxConnectionCount()),
		poolLen:            0,
		connectRand:        rand.NewSource(seed),
		healthCheckRand:    rand.NewSource(seed + 1),
		newConn:            globalNewConn,
		healthCheckNewConn: healthCheck,
		healthCheck:        healthCheck,
		sleepConnect:       time.Sleep,
		sleepHealth:        time.Sleep,
		sleepHealthRetry:   time.Sleep,
	}

	return p
}

func (p *connPool) Open() {
	p.Lock()
	defer p.Unlock()

	if p.status != statusNotOpen {
		return
	}

	p.status = statusOpen

	connectEvery := p.opts.BackgroundConnectInterval()
	connectStutter := p.opts.BackgroundConnectStutter()
	go p.connectEvery(connectEvery, connectStutter)

	healthCheckEvery := p.opts.BackgroundHealthCheckInterval()
	healthCheckStutter := p.opts.BackgroundHealthCheckStutter()
	go p.healthCheckEvery(healthCheckEvery, healthCheckStutter)
}

func (p *connPool) ConnectionCount() int {
	p.RLock()
	poolLen := p.poolLen
	p.RUnlock()
	return int(poolLen)
}

func (p *connPool) NextClient() (rpc.TChanNode, error) {
	p.RLock()
	if p.status != statusOpen {
		p.RUnlock()
		return nil, errConnectionPoolClosed
	}
	if p.poolLen < 1 {
		p.RUnlock()
		return nil, errConnectionPoolHasNoConnections
	}
	n := atomic.AddInt64(&p.used, 1)
	conn := p.pool[n%p.poolLen]
	p.RUnlock()
	return conn.client, nil
}

func (p *connPool) Close() {
	p.Lock()
	if p.status != statusOpen {
		p.Unlock()
		return
	}
	p.status = statusClosed
	p.Unlock()

	for i := range p.pool {
		p.pool[i].channel.Close()
	}
}

func (p *connPool) connectEvery(interval time.Duration, stutter time.Duration) {
	log := p.opts.InstrumentOptions().Logger()
	target := p.opts.MaxConnectionCount()

	for {
		p.RLock()
		state := p.status
		poolLen := int(p.poolLen)
		p.RUnlock()
		if state != statusOpen {
			return
		}

		address := p.host.Address()

		var wg sync.WaitGroup
		for i := 0; i < target-poolLen; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Create connection
				channel, client, err := p.newConn(channelName, address, p.opts)
				if err != nil {
					log.Debugf("could not connect to %s: %v", address, err)
					return
				}

				// Health check the connection
				if err := p.healthCheckNewConn(client, p.opts); err != nil {
					log.Debugf("could not connect to %s: failed health check: %v", address, err)
					channel.Close()
					return
				}

				p.Lock()
				if p.status == statusOpen {
					p.pool = append(p.pool, conn{channel, client})
					p.poolLen = int64(len(p.pool))
				}
				p.Unlock()
			}()
		}

		wg.Wait()

		p.sleepConnect(interval + randStutter(p.connectRand, stutter))
	}
}

func (p *connPool) healthCheckEvery(interval time.Duration, stutter time.Duration) {
	log := p.opts.InstrumentOptions().Logger()
	nowFn := p.opts.ClockOptions().NowFn()

	for {
		p.RLock()
		state := p.status
		p.RUnlock()
		if state != statusOpen {
			return
		}

		var (
			wg       sync.WaitGroup
			start    = nowFn()
			deadline = start.Add(interval + randStutter(p.healthCheckRand, stutter))
		)

		p.RLock()
		for i := int64(0); i < p.poolLen; i++ {
			wg.Add(1)
			go func(client rpc.TChanNode) {
				defer wg.Done()

				var (
					attempts = p.opts.BackgroundHealthCheckFailLimit()
					failed   = 0
					checkErr error
				)
				for j := 0; j < attempts; j++ {
					if err := p.healthCheck(client, p.opts); err != nil {
						checkErr = err
						failed++
						throttleDuration := time.Duration(math.Max(
							float64(time.Second),
							p.opts.BackgroundHealthCheckFailThrottleFactor()*
								float64(p.opts.HostConnectTimeout())))
						p.sleepHealthRetry(throttleDuration)
						continue
					}
					// Healthy
					break
				}

				healthy := failed < attempts
				if !healthy {
					// Log health check error
					log.Debugf("health check failed to %s: %v", p.host.Address(), checkErr)

					// Swap with tail and decrement pool size
					p.Lock()
					if p.status != statusOpen {
						p.Unlock()
						return
					}
					var c conn
					for j := int64(0); j < p.poolLen; j++ {
						if client == p.pool[j].client {
							c = p.pool[j]
							p.pool[j] = p.pool[p.poolLen-1]
							p.pool = p.pool[:p.poolLen-1]
							p.poolLen = int64(len(p.pool))
							break
						}
					}
					p.Unlock()

					// Close the client's channel
					c.channel.Close()
				}
			}(p.pool[i].client)
		}
		p.RUnlock()

		wg.Wait()

		now := nowFn()
		if !now.Before(deadline) {
			// Exceeded deadline, start next health check loop
			p.sleepHealth(0) // Call sleep 0 for tests to intercept this loop continuation
			continue
		}

		p.sleepHealth(deadline.Sub(now))
	}
}

func newConn(channelName string, address string, opts Options) (xclose.SimpleCloser, rpc.TChanNode, error) {
	channel, err := tchannel.NewChannel(channelName, opts.ChannelOptions())
	if err != nil {
		return nil, nil, err
	}
	endpoint := &thrift.ClientOptions{HostPort: address}
	thriftClient := thrift.NewClient(channel, nchannel.ChannelName, endpoint)
	client := rpc.NewTChanNodeClient(thriftClient)
	return channel, client, nil
}

func healthCheck(client rpc.TChanNode, opts Options) error {
	tctx, _ := thrift.NewContext(opts.HostConnectTimeout())
	result, err := client.Health(tctx)
	if err != nil {
		return err
	}
	if !result.Ok {
		return fmt.Errorf("status not ok: %s", result.Status)
	}
	return nil
}

func randStutter(source rand.Source, t time.Duration) time.Duration {
	amount := float64(source.Int63()) / float64(math.MaxInt64)
	return time.Duration(float64(t) * amount)
}
