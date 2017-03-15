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
	"bytes"
	"encoding/json"
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3db/encoding/testgen"
	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type series struct {
	ID   ts.ID
	Data []ts.Datapoint
}

type seriesList []series

type readableSeries struct {
	ID   string
	Data []ts.Datapoint
}

type readableSeriesList []readableSeries

func (l seriesList) Len() int      { return len(l) }
func (l seriesList) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l seriesList) Less(i, j int) bool {
	return bytes.Compare(l[i].ID.Data().Get(), l[j].ID.Data().Get()) < 0
}

func generateTestData(names []string, numPoints int, start time.Time) seriesList {
	if numPoints <= 0 {
		return nil
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	testData := make(seriesList, len(names))
	for i, name := range names {
		datapoints := make([]ts.Datapoint, 0, numPoints)
		for j := 0; j < numPoints; j++ {
			timestamp := start.Add(time.Duration(j) * time.Second)
			datapoints = append(datapoints, ts.Datapoint{
				Timestamp: timestamp,
				Value:     testgen.GenerateFloatVal(r, 3, 1),
			})
		}
		testData[i] = series{
			ID:   ts.StringID(name),
			Data: datapoints,
		}
	}
	return testData
}

func generateTestDataByStart(input []testData) map[time.Time]seriesList {
	seriesMaps := make(map[time.Time]seriesList)
	for _, data := range input {
		generated := generateTestData(data.ids, data.numPoints, data.start)
		seriesMaps[data.start] = generated
	}
	return seriesMaps
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
	namespace ts.ID,
	expected seriesList,
	expectedDebugFilePath string,
	actualDebugFilePath string,
) {
	actual := make(seriesList, len(expected))
	req := rpc.NewFetchRequest()
	for i := range expected {
		s := &expected[i]
		req.NameSpace = namespace.String()
		req.ID = s.ID.String()
		req.RangeStart = xtime.ToNormalizedTime(start, time.Second)
		req.RangeEnd = xtime.ToNormalizedTime(end, time.Second)
		req.ResultTimeType = rpc.TimeType_UNIX_SECONDS
		fetched, err := ts.fetch(req)
		require.NoError(t, err)
		actual[i] = series{
			ID:   s.ID,
			Data: fetched,
		}
	}

	if len(expectedDebugFilePath) > 0 {
		writeVerifyDebugOutput(t, expectedDebugFilePath, start, end, expected)
	}
	if len(actualDebugFilePath) > 0 {
		writeVerifyDebugOutput(t, actualDebugFilePath, start, end, actual)
	}

	require.Equal(t, expected, actual)
}

func writeVerifyDebugOutput(t *testing.T, filePath string, start, end time.Time, series seriesList) {
	w, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	require.NoError(t, err)

	list := make(readableSeriesList, 0, len(series))
	for i := range series {
		list = append(list, readableSeries{ID: series[i].ID.String(), Data: series[i].Data})
	}

	data, err := json.MarshalIndent(struct {
		Start  time.Time
		End    time.Time
		Series readableSeriesList
	}{
		Start:  start,
		End:    end,
		Series: list,
	}, "", "    ")
	require.NoError(t, err)

	_, err = w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func verifySeriesMaps(
	t *testing.T,
	ts *testSetup,
	namespace ts.ID,
	seriesMaps map[time.Time]seriesList,
) {
	debugFilePathPrefix := ts.opts.VerifySeriesDebugFilePathPrefix()
	expectedDebugFilePath := createFileIfPrefixSet(t, debugFilePathPrefix, "expected.log")
	actualDebugFilePath := createFileIfPrefixSet(t, debugFilePathPrefix, "actual.log")

	for timestamp, sm := range seriesMaps {
		start := timestamp
		end := timestamp.Add(ts.storageOpts.RetentionOptions().BlockSize())
		verifySeriesMapForRange(
			t, ts, start, end, namespace, sm,
			expectedDebugFilePath, actualDebugFilePath)
	}
}

func createFileIfPrefixSet(t *testing.T, prefix, suffix string) string {
	if len(prefix) == 0 {
		return ""
	}
	filePath := prefix + "_" + suffix
	w, err := os.Create(filePath)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return filePath
}

func compareSeriesList(
	t *testing.T,
	expected seriesList,
	actual seriesList,
) {
	sort.Sort(expected)
	sort.Sort(actual)

	require.Equal(t, len(expected), len(actual))

	for i := range expected {
		require.Equal(t, expected[i].ID.Data().Get(), actual[i].ID.Data().Get())
		require.Equal(t, expected[i].Data, expected[i].Data)
	}
}
