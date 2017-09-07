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

package integration

import (
	"fmt"
	"net"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/protocol/msgpack"
)

type client struct {
	address        string
	batchSize      int
	connectTimeout time.Duration
	encoder        msgpack.UnaggregatedEncoder
	conn           net.Conn
}

func newClient(address string, batchSize int, connectTimeout time.Duration) *client {
	return &client{
		address:        address,
		batchSize:      batchSize,
		connectTimeout: connectTimeout,
		encoder:        msgpack.NewUnaggregatedEncoder(msgpack.NewPooledBufferedEncoder(nil)),
	}
}

func (c *client) connect() error {
	conn, err := net.DialTimeout("tcp", c.address, c.connectTimeout)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *client) testConnection() bool {
	if err := c.connect(); err != nil {
		return false
	}
	c.conn.Close()
	c.conn = nil
	return true
}

func (c *client) write(mu unaggregated.MetricUnion, pl policy.PoliciesList) error {
	encoder := c.encoder.Encoder()
	sizeBefore := encoder.Buffer().Len()
	var err error
	switch mu.Type {
	case unaggregated.CounterType:
		err = c.encoder.EncodeCounterWithPoliciesList(unaggregated.CounterWithPoliciesList{
			Counter:      mu.Counter(),
			PoliciesList: pl,
		})
	case unaggregated.BatchTimerType:
		err = c.encoder.EncodeBatchTimerWithPoliciesList(unaggregated.BatchTimerWithPoliciesList{
			BatchTimer:   mu.BatchTimer(),
			PoliciesList: pl,
		})
	case unaggregated.GaugeType:
		err = c.encoder.EncodeGaugeWithPoliciesList(unaggregated.GaugeWithPoliciesList{
			Gauge:        mu.Gauge(),
			PoliciesList: pl,
		})
	default:
		err = fmt.Errorf("unrecognized metric type %v", mu.Type)
	}
	if err != nil {
		encoder.Buffer().Truncate(sizeBefore)
		c.encoder.Reset(encoder)
		return err
	}
	sizeAfter := encoder.Buffer().Len()
	// If the buffer size is not big enough, do nothing
	if sizeAfter < c.batchSize {
		return nil
	}
	// Otherwise we get a new buffer and copy the bytes exceeding the max
	// flush size to it, swap the new buffer with the old one, and flush out
	// the old buffer
	encoder2 := msgpack.NewPooledBufferedEncoder(nil)
	data := encoder.Bytes()
	encoder2.Buffer().Write(data[sizeBefore:sizeAfter])
	c.encoder.Reset(encoder2)
	encoder.Buffer().Truncate(sizeBefore)
	_, err = c.conn.Write(encoder.Bytes())
	encoder.Close()
	return err
}

func (c *client) flush() error {
	if encoder := c.encoder.Encoder(); len(encoder.Bytes()) > 0 {
		c.encoder.Reset(msgpack.NewPooledBufferedEncoder(nil))
		_, err := c.conn.Write(encoder.Bytes())
		encoder.Close()
		return err
	}
	return nil
}

func (c *client) close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}
