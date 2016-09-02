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
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3db/encoding/testgen"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type series struct {
	id   string
	data []ts.Datapoint
}

type seriesList []series

func (l seriesList) Len() int      { return len(l) }
func (l seriesList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l seriesList) Less(i, j int) bool {
	return strings.Compare(l[i].id, l[j].id) < 0
}

func generateTestData(names []string, numPoints int, start time.Time) seriesList {
	if numPoints <= 0 {
		return nil
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	testData := make(seriesList, len(names))
	for i, name := range names {
		datapoints := make([]ts.Datapoint, 0, numPoints)
		for i := 0; i < numPoints; i++ {
			timestamp := start.Add(time.Duration(i) * time.Second)
			datapoints = append(datapoints, ts.Datapoint{
				Timestamp: timestamp,
				Value:     testgen.GenerateFloatVal(r, 3, 1),
			})
		}
		testData[i] = series{
			id:   name,
			data: datapoints,
		}
	}

	return testData
}

func toDatapoints(fetched *rpc.FetchResult_) []ts.Datapoint {
	converted := make([]ts.Datapoint, len(fetched.Datapoints))
	for i, dp := range fetched.Datapoints {
		converted[i] = ts.Datapoint{
			Timestamp: xtime.FromNormalizedTime(dp.Timestamp, time.Second),
			Value:     dp.Value,
		}
	}
	return converted
}

func verifySeriesMapForRange(
	t *testing.T,
	ts *testSetup,
	start, end time.Time,
	namespace string,
	expected seriesList,
) {
	actual := make(seriesList, len(expected))
	req := rpc.NewFetchRequest()
	for i, s := range expected {
		req.NameSpace = namespace
		req.ID = s.id
		req.RangeStart = xtime.ToNormalizedTime(start, time.Second)
		req.RangeEnd = xtime.ToNormalizedTime(end, time.Second)
		req.ResultTimeType = rpc.TimeType_UNIX_SECONDS
		fetched, err := ts.fetch(req)
		require.NoError(t, err)
		actual[i] = series{
			id:   s.id,
			data: fetched,
		}
	}
	require.Equal(t, expected, actual)
}

func verifySeriesMaps(
	t *testing.T,
	ts *testSetup,
	namespace string,
	seriesMaps map[time.Time]seriesList,
) {
	for timestamp, sm := range seriesMaps {
		start := timestamp
		end := timestamp.Add(ts.storageOpts.GetRetentionOptions().GetBlockSize())
		verifySeriesMapForRange(t, ts, start, end, namespace, sm)
	}
}

func compareSeriesList(
	t *testing.T,
	expected seriesList,
	actual seriesList,
) {
	sort.Sort(expected)
	sort.Sort(actual)
	require.Equal(t, expected, actual)
}
