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
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"

	"github.com/spaolacci/murmur3"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const (
	channelName        = "Client"
	connectEvery       = 10 * time.Second
	connectStutter     = 5 * time.Second
	healthCheckEvery   = 5 * time.Second
	healthCheckStutter = 5 * time.Second
)

var (
	errConnectionPoolClosed           = errors.New("connection pool closed")
	errConnectionPoolHasNoConnections = errors.New("connection pool has no connections")
)

type connectionPool interface {
	// GetConnectionCount gets the current open connection count
	GetConnectionCount() int

	// NextClient gets the next client for use by the connection pool
	NextClient() (rpc.TChanNode, error)

	// Close the connection pool
	Close()
}

type connPool struct {
	sync.RWMutex

	opts            m3db.ClientOptions
	host            m3db.Host
	pool            []conn
	poolLen         int64
	used            int64
	connectRand     rand.Source
	healthCheckRand rand.Source
	closed          bool
}

type conn struct {
	channel *tchannel.Channel
	client  rpc.TChanNode
}

func newConnectionPool(host m3db.Host, opts m3db.ClientOptions) connectionPool {
	seed := int64(murmur3.Sum32([]byte(host.Address())))

	p := &connPool{
		opts:            opts,
		host:            host,
		pool:            make([]conn, 0, opts.GetMaxConnectionCount()),
		poolLen:         0,
		connectRand:     rand.NewSource(seed),
		healthCheckRand: rand.NewSource(seed + 1),
	}

	go p.connectEvery(connectEvery, connectStutter)

	go p.healthCheckEvery(healthCheckEvery, healthCheckStutter)

	return p
}

func (p *connPool) GetConnectionCount() int {
	p.RLock()
	poolLen := p.poolLen
	p.RUnlock()
	return int(poolLen)
}

func (p *connPool) NextClient() (rpc.TChanNode, error) {
	p.RLock()
	if p.closed {
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
	if p.closed {
		p.Unlock()
		return
	}
	p.closed = true
	p.Unlock()

	for i := range p.pool {
		p.pool[i].channel.Close()
	}
}

func (p *connPool) connectEvery(interval time.Duration, stutter time.Duration) {
	log := p.opts.GetLogger()

	for {
		p.RLock()
		closed := p.closed
		poolLen := int(p.poolLen)
		p.RUnlock()
		if closed {
			return
		}

		target := p.opts.GetMaxConnectionCount()
		if poolLen >= target {
			// No need to spawn connections
			time.Sleep(interval + randStutter(p.connectRand, stutter))
			continue
		}

		var wg sync.WaitGroup
		address := p.host.Address()
		for i := 0; i < target-poolLen; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				channel, err := tchannel.NewChannel(channelName, p.opts.GetChannelOptions())
				if err != nil {
					log.Warnf("construct channel error to %s: %v", address, err)
					return
				}

				endpoint := &thrift.ClientOptions{HostPort: address}
				thriftClient := thrift.NewClient(channel, node.ChannelName, endpoint)
				client := rpc.NewTChanNodeClient(thriftClient)

				// Force a request to open the connection
				ctx, _ := thrift.NewContext(p.opts.GetHostConnectTimeout())
				result, err := client.Health(ctx)
				if err != nil {
					log.Warnf("could not connect to %s: %v", address, err)
					return
				}
				if !result.Ok {
					log.Warnf("could not connect to unhealthy host %s: %s", address, result.Status)
					return
				}

				p.Lock()
				if !p.closed {
					p.pool = append(p.pool, conn{channel, client})
					p.poolLen = int64(len(p.pool))
				}
				p.Unlock()
			}()
		}

		wg.Wait()

		time.Sleep(interval + randStutter(p.connectRand, stutter))
	}
}

func (p *connPool) healthCheckEvery(interval time.Duration, stutter time.Duration) {
	log := p.opts.GetLogger()
	nowFn := p.opts.GetNowFn()

	for {
		p.RLock()
		closed := p.closed
		poolLen := int(p.poolLen)
		p.RUnlock()
		if closed {
			return
		}

		start := nowFn()
		deadline := start.Add(interval + randStutter(p.healthCheckRand, stutter))
		// As this is the only loop that removes connections it is safe to loop upwards
		for i := 0; i < poolLen; i++ {
			healthy := true

			p.RLock()
			client := p.pool[i].client
			p.RUnlock()

			tctx, _ := thrift.NewContext(p.opts.GetHostConnectTimeout())
			result, err := client.Health(tctx)
			if err != nil {
				healthy = false
				log.Warnf("health check failed to %s: %v", p.host.Address(), err)
			} else if !result.Ok {
				healthy = false
				log.Warnf("health check found unhealthy host %s: %s", p.host.Address(), result.Status)
			}

			if !healthy {
				// Swap with tail, decrement pool size, decrement "i" to try next in line
				p.Lock()
				if p.closed {
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
			closed := p.closed
			poolLen = int(p.poolLen)
			p.RUnlock()

			if closed {
				return
			}
		}

		now := nowFn()
		if !now.Before(deadline) {
			// Exceeded deadline, start next health check loop
			continue
		}

		time.Sleep(deadline.Sub(now))
	}
}

func randStutter(source rand.Source, t time.Duration) time.Duration {
	amount := float64(source.Int63()) / float64(math.MaxInt64)
	return time.Duration(float64(t) * amount)
}
