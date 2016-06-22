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

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	xclose "github.com/m3db/m3db/x/close"

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

type connectionPool interface {
	// Open starts the connection pool connecting and health checking
	Open()

	// GetConnectionCount gets the current open connection count
	GetConnectionCount() int

	// NextClient gets the next client for use by the connection pool
	NextClient() (rpc.TChanNode, error)

	// Close the connection pool
	Close()
}

type connPool struct {
	sync.RWMutex

	opts               m3db.ClientOptions
	host               m3db.Host
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
	state              state
}

type conn struct {
	channel xclose.SimpleCloser
	client  rpc.TChanNode
}

type newConnFn func(channelName string, addr string, opts m3db.ClientOptions) (xclose.SimpleCloser, rpc.TChanNode, error)

type healthCheckFn func(client rpc.TChanNode, opts m3db.ClientOptions) error

type sleepFn func(t time.Duration)

func newConnectionPool(host m3db.Host, opts m3db.ClientOptions) connectionPool {
	seed := int64(murmur3.Sum32([]byte(host.Address())))

	p := &connPool{
		opts:               opts,
		host:               host,
		pool:               make([]conn, 0, opts.GetMaxConnectionCount()),
		poolLen:            0,
		connectRand:        rand.NewSource(seed),
		healthCheckRand:    rand.NewSource(seed + 1),
		newConn:            newConn,
		healthCheckNewConn: healthCheck,
		healthCheck:        healthCheck,
		sleepConnect:       time.Sleep,
		sleepHealth:        time.Sleep,
	}

	return p
}

func (p *connPool) Open() {
	p.Lock()
	defer p.Unlock()

	if p.state != stateNotOpen {
		return
	}

	p.state = stateOpen

	connectEvery := p.opts.GetBackgroundConnectInterval()
	connectStutter := p.opts.GetBackgroundConnectStutter()
	go p.connectEvery(connectEvery, connectStutter)

	healthCheckEvery := p.opts.GetBackgroundHealthCheckInterval()
	healthCheckStutter := p.opts.GetBackgroundHealthCheckStutter()
	go p.healthCheckEvery(healthCheckEvery, healthCheckStutter)
}

func (p *connPool) GetConnectionCount() int {
	p.RLock()
	poolLen := p.poolLen
	p.RUnlock()
	return int(poolLen)
}

func (p *connPool) NextClient() (rpc.TChanNode, error) {
	p.RLock()
	if p.state != stateOpen {
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
	if p.state != stateOpen {
		p.Unlock()
		return
	}
	p.state = stateClosed
	p.Unlock()

	for i := range p.pool {
		p.pool[i].channel.Close()
	}
}

func (p *connPool) connectEvery(interval time.Duration, stutter time.Duration) {
	log := p.opts.GetLogger()
	target := p.opts.GetMaxConnectionCount()

	for {
		p.RLock()
		state := p.state
		poolLen := int(p.poolLen)
		p.RUnlock()
		if state != stateOpen {
			return
		}

		if poolLen >= target {
			// No need to spawn connections
			p.sleepConnect(interval + randStutter(p.connectRand, stutter))
			continue
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
					log.Warnf("could not connect to %s: %v", address, err)
					return
				}

				// Health check the connection
				if err := p.healthCheckNewConn(client, p.opts); err != nil {
					log.Warnf("could not connect to %s: failed health check: %v", address, err)
					return
				}

				p.Lock()
				if p.state == stateOpen {
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
	log := p.opts.GetLogger()
	nowFn := p.opts.GetNowFn()

	for {
		p.RLock()
		state := p.state
		poolLen := int(p.poolLen)
		p.RUnlock()
		if state != stateOpen {
			return
		}

		start := nowFn()
		deadline := start.Add(interval + randStutter(p.healthCheckRand, stutter))
		// As this is the only loop that removes connections it is safe to loop upwards
		for i := 0; i < poolLen; i++ {
			p.RLock()
			client := p.pool[i].client
			p.RUnlock()

			healthy := true
			if err := p.healthCheck(client, p.opts); err != nil {
				healthy = false
				log.Warnf("health check failed to %s: %v", p.host.Address(), err)
			}

			if !healthy {
				// Swap with tail, decrement pool size, decrement "i" to try next in line
				p.Lock()
				if p.state != stateOpen {
					p.Unlock()
					return
				}
				p.pool[i], p.pool[p.poolLen-1] = p.pool[p.poolLen-1], p.pool[i]
				p.pool = p.pool[:p.poolLen-1]
				p.poolLen = int64(len(p.pool))
				p.Unlock()

				// Retry with the element we just swapped in
				i--
			}

			// Bail early if closed, reset poolLen in case we decremented or conns got added
			p.RLock()
			state := p.state
			poolLen = int(p.poolLen)
			p.RUnlock()

			if state != stateOpen {
				return
			}
		}

		now := nowFn()
		if !now.Before(deadline) {
			// Exceeded deadline, start next health check loop
			p.sleepHealth(0) // Call sleep 0 for tests to intercept this loop continuation
			continue
		}

		p.sleepHealth(deadline.Sub(now))
	}
}

func newConn(channelName string, address string, opts m3db.ClientOptions) (xclose.SimpleCloser, rpc.TChanNode, error) {
	channel, err := tchannel.NewChannel(channelName, opts.GetChannelOptions())
	if err != nil {
		return nil, nil, err
	}
	endpoint := &thrift.ClientOptions{HostPort: address}
	thriftClient := thrift.NewClient(channel, node.ChannelName, endpoint)
	client := rpc.NewTChanNodeClient(thriftClient)
	return channel, client, nil
}

func healthCheck(client rpc.TChanNode, opts m3db.ClientOptions) error {
	tctx, _ := thrift.NewContext(opts.GetHostConnectTimeout())
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
