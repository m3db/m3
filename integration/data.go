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
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/network/server/tchannelthrift/thrift/gen-go/rpc"
	xtime "github.com/m3db/m3db/x/time"

	"github.com/stretchr/testify/require"
)

type byTimestampAscending []m3db.Datapoint

func (asc byTimestampAscending) Len() int           { return len(asc) }
func (asc byTimestampAscending) Swap(i, j int)      { asc[i], asc[j] = asc[j], asc[i] }
func (asc byTimestampAscending) Less(i, j int) bool { return asc[i].Timestamp.Before(asc[j].Timestamp) }

type dataMap map[string][]m3db.Datapoint

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

func verifyDataMapForRange(
	t *testing.T,
	ts *testSetup,
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
		fetched, err := ts.fetch(req)
		require.NoError(t, err)
		converted := make([]m3db.Datapoint, len(fetched.Datapoints))
		for i, dp := range fetched.Datapoints {
			converted[i] = m3db.Datapoint{
				Timestamp: xtime.FromNormalizedTime(dp.Timestamp, time.Second),
				Value:     dp.Value,
			}
		}
		// TODO(xichen): sorting is necessary in that the fetch call may return
		// out-of-order datapoints at the moment. Remove this once the client read
		// diff lands.
		sort.Sort(byTimestampAscending(converted))
		actual[id] = converted
	}
	require.Equal(t, expected, actual)
}

func verifyDataMaps(
	t *testing.T,
	ts *testSetup,
	dataMaps map[time.Time]dataMap,
) {
	for timestamp, dm := range dataMaps {
		start := timestamp
		end := timestamp.Add(ts.dbOpts.GetBlockSize())
		verifyDataMapForRange(t, ts, start, end, dm)
	}
}
