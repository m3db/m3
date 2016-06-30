// +build integration

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
	"flag"
	"io/ioutil"
	"testing"
	"time"

	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/node"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	"github.com/m3db/m3db/services/m3dbnode/server"
	"github.com/m3db/m3db/storage"
	xclose "github.com/m3db/m3db/x/close"
	xtime "github.com/m3db/m3db/x/time"
	"github.com/stretchr/testify/require"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

// TODO(xichen): if this gets too unwieldy, we can consider making these
// test options or loading configurations from a config file.
const (
	// channelName is the channel name.
	channelName = "integration-test"

	// readRequestTimeout is the read request timeout.
	readRequestTimeout = 5 * time.Second

	// writeRequestTimeout is the write request timeout.
	writeRequestTimeout = 5 * time.Second

	// serverStateChangeTimeout is the time we wait for a server to change its state.
	serverStateChangeTimeout = 30 * time.Second
)

var (
	httpClusterAddr        = flag.String("clusterhttpaddr", "0.0.0.0:9000", "Cluster HTTP server address")
	tchannelClusterAddr    = flag.String("clustertchanneladdr", "0.0.0.0:9001", "Cluster TChannel server address")
	httpNodeAddr           = flag.String("nodehttpaddr", "0.0.0.0:9002", "Node HTTP server address")
	tchannelNodeAddr       = flag.String("nodetchanneladdr", "0.0.0.0:9003", "Node TChannel server address")
	errServerStartTimedOut = errors.New("server took too long to start")
	errServerStopTimedOut  = errors.New("server took too long to stop")
)

type dataMap map[string][]m3db.Datapoint

func databaseOptions() m3db.DatabaseOptions {
	var opts m3db.DatabaseOptions
	opts = storage.NewDatabaseOptions().NewBootstrapFn(func() m3db.Bootstrap {
		return bootstrap.NewNoOpBootstrapProcess(opts)
	})
	return opts
}

func setup() (m3db.DatabaseOptions, *time.Time, error) {
	dbOpts := databaseOptions()

	// set up now fn
	now := time.Now().Truncate(dbOpts.GetBlockSize())
	nowFn := func() time.Time { return now }
	dbOpts = dbOpts.NowFn(nowFn)

	// set up file path
	dir, err := ioutil.TempDir("", "integration-test")
	if err != nil {
		return nil, nil, err
	}
	dbOpts = dbOpts.FilePathPrefix(dir)

	return dbOpts, &now, nil
}

type conditionFn func() bool

func waitUntil(fn conditionFn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func waitUntilServerIsUp(address string) error {
	fakeRequest := rpc.NewFetchRequest()
	serverIsUp := func() bool { _, err := fetch(fakeRequest); return err == nil }
	if waitUntil(serverIsUp, serverStateChangeTimeout) {
		return nil
	}
	return errServerStartTimedOut
}

func waitUntilServerIsDown(address string) error {
	fakeRequest := rpc.NewFetchRequest()
	serverIsDown := func() bool { _, err := fetch(fakeRequest); return err != nil }
	if waitUntil(serverIsDown, serverStateChangeTimeout) {
		return nil
	}
	return errServerStopTimedOut
}

func startServer(opts m3db.DatabaseOptions, doneCh chan struct{}) error {
	go server.Serve(*httpClusterAddr, *tchannelClusterAddr, *httpNodeAddr, *tchannelNodeAddr, opts, doneCh)

	return waitUntilServerIsUp(*tchannelNodeAddr)
}

func stopServer(doneCh chan<- struct{}) error {
	doneCh <- struct{}{}

	return waitUntilServerIsDown(*tchannelNodeAddr)
}

func generateTestData(metricNames []string, numPoints int, start time.Time) dataMap {
	if numPoints <= 0 {
		return nil
	}
	testData := make(map[string][]m3db.Datapoint)
	for _, name := range metricNames {
		datapoints := make([]m3db.Datapoint, 0, numPoints)
		for i := 0; i < numPoints; i++ {
			timestamp := start.Add(time.Duration(i) * time.Second)
			datapoints = append(datapoints, m3db.Datapoint{
				Timestamp: timestamp,
				Value:     0.1 * float64(i),
			})
		}
		testData[name] = datapoints
	}
	return testData
}

func tchannelClient(address string) (xclose.SimpleCloser, rpc.TChanNode, error) {
	channel, err := tchannel.NewChannel(channelName, nil)
	if err != nil {
		return nil, nil, err
	}
	endpoint := &thrift.ClientOptions{HostPort: address}
	thriftClient := thrift.NewClient(channel, node.ChannelName, endpoint)
	client := rpc.NewTChanNodeClient(thriftClient)
	return channel, client, nil
}

func writeBatch(dm dataMap) error {
	channel, client, err := tchannelClient(*tchannelNodeAddr)
	if err != nil {
		return err
	}
	defer channel.Close()

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

	ctx, _ := thrift.NewContext(writeRequestTimeout)
	batchReq := &rpc.WriteBatchRequest{Elements: elems}
	return client.WriteBatch(ctx, batchReq)
}

func fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error) {
	channel, client, err := tchannelClient(*tchannelNodeAddr)
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	ctx, _ := thrift.NewContext(readRequestTimeout)
	return client.Fetch(ctx, req)
}

func verifyDataMapForRange(
	t *testing.T,
	start, end time.Time,
	expected dataMap,
) {
	actual := make(dataMap, len(expected))
	req := rpc.NewFetchRequest()
	for id := range expected {
		req.ID = id
		req.RangeStart = xtime.ToNormalizedTime(start, time.Second)
		req.RangeEnd = xtime.ToNormalizedTime(end, time.Second)
		req.ResultTimeType = rpc.TimeType_UNIX_SECONDS
		fetched, err := fetch(req)
		require.NoError(t, err)
		converted := make([]m3db.Datapoint, len(fetched.Datapoints))
		for i, dp := range fetched.Datapoints {
			converted[i] = m3db.Datapoint{
				Timestamp: xtime.FromNormalizedTime(dp.Timestamp, time.Second),
				Value:     dp.Value,
			}
		}
		actual[id] = converted
	}
	require.Equal(t, expected, actual)
}

func verifyDataMaps(
	t *testing.T,
	blockSize time.Duration,
	dataMaps map[time.Time]dataMap,
) {
	for timestamp, dm := range dataMaps {
		start := timestamp
		end := timestamp.Add(blockSize)
		verifyDataMapForRange(t, start, end, dm)
	}
}
