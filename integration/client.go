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
	"sync"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3db/services/m3dbnode/server"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

func tchannelClient(address string) (*tchannel.Channel, rpc.TChanNode, error) {
	channel, err := tchannel.NewChannel("integration-test", nil)
	if err != nil {
		return nil, nil, err
	}
	endpoint := &thrift.ClientOptions{HostPort: address}
	thriftClient := thrift.NewClient(channel, node.ChannelName, endpoint)
	client := rpc.NewTChanNodeClient(thriftClient)
	return channel, client, nil
}

// tchannelClientWriteBatch writes a data map using a tchannel client.
func tchannelClientWriteBatch(client rpc.TChanNode, timeout time.Duration, dm dataMap) error {
	var elems []*rpc.WriteRequest
	for name, datapoints := range dm {
		for _, dp := range datapoints {
			req := &rpc.WriteRequest{
				ID: name,
				Datapoint: &rpc.Datapoint{
					Timestamp:     xtime.ToNormalizedTime(dp.Timestamp, time.Second),
					Value:         dp.Value,
					TimestampType: rpc.TimeType_UNIX_SECONDS,
				},
			}
			elems = append(elems, req)
		}
	}

	ctx, _ := thrift.NewContext(timeout)
	batchReq := &rpc.WriteBatchRequest{Elements: elems}
	return client.WriteBatch(ctx, batchReq)
}

// tchannelClientFetch fulfills a fetch request using a tchannel client.
func tchannelClientFetch(client rpc.TChanNode, timeout time.Duration, req *rpc.FetchRequest) ([]m3db.Datapoint, error) {
	ctx, _ := thrift.NewContext(timeout)
	fetched, err := client.Fetch(ctx, req)
	if err != nil {
		return nil, err
	}
	return toDatapoints(fetched), nil
}

func m3dbClient(address string, shardingScheme m3db.ShardScheme) (m3db.Client, error) {
	return server.DefaultClient(address, shardingScheme)
}

// m3dbClientWriteBatch writes a data map using an m3db client.
func m3dbClientWriteBatch(client m3db.Client, workerPool m3db.WorkerPool, dm dataMap) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	var (
		errCh = make(chan error, 1)
		wg    sync.WaitGroup
	)

	for name, datapoints := range dm {
		for _, dp := range datapoints {
			wg.Add(1)
			n, d := name, dp
			workerPool.Go(func() {
				defer wg.Done()

				if err := session.Write(n, d.Timestamp, d.Value, xtime.Second, nil); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			})
		}
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return err
	}
	return nil
}

// m3dbClientFetch fulfills a fetch request using an m3db client.
func m3dbClientFetch(client m3db.Client, req *rpc.FetchRequest) ([]m3db.Datapoint, error) {
	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	iter, err := session.Fetch(
		req.ID,
		xtime.FromNormalizedTime(req.RangeStart, time.Second),
		xtime.FromNormalizedTime(req.RangeEnd, time.Second),
	)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var datapoints []m3db.Datapoint
	for iter.Next() {
		dp, _, _ := iter.Current()
		datapoints = append(datapoints, dp)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return datapoints, nil
}
