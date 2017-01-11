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
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/generated/thrift/rpc"
	nchannel "github.com/m3db/m3db/network/server/tchannelthrift/node/channel"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/sync"
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
	thriftClient := thrift.NewClient(channel, nchannel.ChannelName, endpoint)
	client := rpc.NewTChanNodeClient(thriftClient)
	return channel, client, nil
}

// tchannelClientWriteBatch writes a data map using a tchannel client.
func tchannelClientWriteBatch(client rpc.TChanNode, timeout time.Duration, namespace ts.ID, seriesList seriesList) error {
	var elems []*rpc.WriteBatchRawRequestElement
	for _, series := range seriesList {
		for _, dp := range series.Data {
			elem := &rpc.WriteBatchRawRequestElement{
				ID: series.ID.Data().Get(),
				Datapoint: &rpc.Datapoint{
					Timestamp:     xtime.ToNormalizedTime(dp.Timestamp, time.Second),
					Value:         dp.Value,
					TimestampType: rpc.TimeType_UNIX_SECONDS,
				},
			}
			elems = append(elems, elem)
		}
	}

	ctx, _ := thrift.NewContext(timeout)
	batchReq := &rpc.WriteBatchRawRequest{NameSpace: namespace.Data().Get(), Elements: elems}
	return client.WriteBatchRaw(ctx, batchReq)
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

// tchannelClientTruncate fulfills a namespace truncation request using a tchannel client.
func tchannelClientTruncate(client rpc.TChanNode, timeout time.Duration, req *rpc.TruncateRequest) (int64, error) {
	ctx, _ := thrift.NewContext(timeout)
	truncated, err := client.Truncate(ctx, req)
	if err != nil {
		return 0, err
	}
	return truncated.NumSeries, nil
}

func tchannelClientHealth(client rpc.TChanNode) (*rpc.NodeHealthResult_, error) {
	ctx, _ := thrift.NewContext(5 * time.Second)
	return client.Health(ctx)
}

func m3dbClient(opts client.Options) (client.Client, error) {
	return client.NewClient(opts)
}

// m3dbClientWriteBatch writes a data map using an m3db client.
func m3dbClientWriteBatch(client client.Client, workerPool xsync.WorkerPool, namespace ts.ID, seriesList seriesList) error {
	session, err := client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()

	var (
		errCh = make(chan error, 1)
		wg    sync.WaitGroup
	)

	for _, series := range seriesList {
		for _, dp := range series.Data {
			wg.Add(1)
			id, d := series.ID, dp
			workerPool.Go(func() {
				defer wg.Done()

				if err := session.Write(
					namespace.String(), id.String(), d.Timestamp, d.Value, xtime.Second, nil); err != nil {
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
		req.NameSpace,
		req.ID,
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

// m3dbClientTruncate fulfills a truncation request using an m3db client.
func m3dbClientTruncate(c client.Client, req *rpc.TruncateRequest) (int64, error) {
	session, err := c.NewSession()
	if err != nil {
		return 0, err
	}
	defer session.Close()

	adminSession, ok := session.(client.AdminSession)
	if !ok {
		return 0, errors.New("unable to get an admin session")
	}

	return adminSession.Truncate(ts.BinaryID(checked.NewBytes(req.NameSpace, nil)))
}
