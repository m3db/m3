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
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/ts"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type readableSeries struct {
	ID   string
	Data []ts.Datapoint
}

type readableSeriesList []readableSeries

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
	expected generate.SeriesBlock,
	expectedDebugFilePath string,
	actualDebugFilePath string,
) {
	actual := make(generate.SeriesBlock, len(expected))
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
		actual[i] = generate.Series{
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

func writeVerifyDebugOutput(t *testing.T, filePath string, start, end time.Time, series generate.SeriesBlock) {
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

// nolint: deadcode
func verifySeriesMaps(
	t *testing.T,
	ts *testSetup,
	namespace ts.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) {
	debugFilePathPrefix := ts.opts.VerifySeriesDebugFilePathPrefix()
	expectedDebugFilePath := createFileIfPrefixSet(t, debugFilePathPrefix, fmt.Sprintf("%s-expected.log", namespace.String()))
	actualDebugFilePath := createFileIfPrefixSet(t, debugFilePathPrefix, fmt.Sprintf("%s-actual.log", namespace.String()))

	nsMetadata, ok := ts.db.Namespace(namespace)
	require.True(t, ok)
	nsOpts := nsMetadata.Options()

	for timestamp, sm := range seriesMaps {
		start := timestamp.ToTime()
		end := start.Add(nsOpts.RetentionOptions().BlockSize())
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

// nolint: deadcode
func compareSeriesList(
	t *testing.T,
	expected generate.SeriesBlock,
	actual generate.SeriesBlock,
) {
	sort.Sort(expected)
	sort.Sort(actual)

	require.Equal(t, len(expected), len(actual))

	for i := range expected {
		require.Equal(t, expected[i].ID.Data().Get(), actual[i].ID.Data().Get())
		require.Equal(t, expected[i].Data, expected[i].Data)
	}
}
