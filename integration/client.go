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

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/pool"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

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
func tchannelClientWriteBatch(client rpc.TChanNode, timeout time.Duration, dm seriesList) error {
	var elems []*rpc.WriteRequest
	for _, series := range dm {
		for _, dp := range series.data {
			req := &rpc.WriteRequest{
				IdWithNamespace: &rpc.IDWithNamespace{ID: series.id, Ns: series.namespace},
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
func tchannelClientFetch(client rpc.TChanNode, timeout time.Duration, req *rpc.FetchRequest) ([]ts.Datapoint, error) {
	ctx, _ := thrift.NewContext(timeout)
	fetched, err := client.Fetch(ctx, req)
	if err != nil {
		return nil, err
	}
	return toDatapoints(fetched), nil
}

// tchannelClientTruncateNamespace fulfills a namespace truncation request using a tchannel client.
func tchannelClientTruncateNamespace(client rpc.TChanNode, timeout time.Duration, req *rpc.TruncateNamespaceRequest) error {
	ctx, _ := thrift.NewContext(timeout)
	return client.TruncateNamespace(ctx, req)
}

func m3dbClient(opts client.Options) (client.Client, error) {
	return client.NewClient(opts)
}

// m3dbClientWriteBatch writes a data map using an m3db client.
func m3dbClientWriteBatch(client client.Client, workerPool pool.WorkerPool, dm seriesList) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	var (
		errCh = make(chan error, 1)
		wg    sync.WaitGroup
	)

	for _, series := range dm {
		for _, dp := range series.data {
			wg.Add(1)
			ns, id, d := series.namespace, series.id, dp
			workerPool.Go(func() {
				defer wg.Done()

				if err := session.Write(ns, id, d.Timestamp, d.Value, xtime.Second, nil); err != nil {
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
func m3dbClientFetch(client client.Client, req *rpc.FetchRequest) ([]ts.Datapoint, error) {
	session, err := client.NewSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	iter, err := session.Fetch(
		req.IdWithNamespace.Ns,
		req.IdWithNamespace.ID,
		xtime.FromNormalizedTime(req.RangeStart, time.Second),
		xtime.FromNormalizedTime(req.RangeEnd, time.Second),
	)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var datapoints []ts.Datapoint
	for iter.Next() {
		dp, _, _ := iter.Current()
		datapoints = append(datapoints, dp)
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return datapoints, nil
}
