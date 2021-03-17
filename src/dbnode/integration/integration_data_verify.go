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
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type readableSeries struct {
	ID   string
	Tags []readableSeriesTag
	Data []generate.TestValue
}

type readableSeriesTag struct {
	Name  string
	Value string
}

type readableSeriesList []readableSeries

func toDatapoints(fetched *rpc.FetchResult_) []generate.TestValue {
	converted := make([]generate.TestValue, len(fetched.Datapoints))
	for i, dp := range fetched.Datapoints {
		converted[i] = generate.TestValue{
			Datapoint: ts.Datapoint{
				Timestamp:      xtime.FromNormalizedTime(dp.Timestamp, time.Second),
				TimestampNanos: xtime.ToUnixNano(xtime.FromNormalizedTime(dp.Timestamp, time.Second)),
				Value:          dp.Value,
			},
			Annotation: dp.Annotation,
		}
	}
	return converted
}

func verifySeriesMapForRange(
	t *testing.T,
	ts TestSetup,
	start, end time.Time,
	namespace ident.ID,
	input generate.SeriesBlock,
	expectedDebugFilePath string,
	actualDebugFilePath string,
) bool {
	// Construct a copy of the input that we will use to compare
	// with only the fields we need to compare against (fetch doesn't
	// return the tags for a series ID)
	expected := make(generate.SeriesBlock, len(input))
	actual := make(generate.SeriesBlock, len(input))

	expectedMetadata := map[string]generate.Series{}
	req := rpc.NewFetchRequest()
	for i := range input {
		idString := input[i].ID.String()
		req.NameSpace = namespace.String()
		req.ID = idString
		req.RangeStart = xtime.ToNormalizedTime(start, time.Second)
		req.RangeEnd = xtime.ToNormalizedTime(end, time.Second)
		req.ResultTimeType = rpc.TimeType_UNIX_SECONDS
		fetched, err := ts.Fetch(req)

		if !assert.NoError(t, err) {
			return false
		}
		expected[i] = generate.Series{
			ID:   input[i].ID,
			Data: input[i].Data,
		}
		actual[i] = generate.Series{
			ID:   input[i].ID,
			Data: fetched,
		}

		// Build expected metadata map at the same time
		expectedMetadata[idString] = input[i]
	}

	if len(expectedDebugFilePath) > 0 {
		if !writeVerifyDebugOutput(t, expectedDebugFilePath, start, end, expected) {
			return false
		}
	}
	if len(actualDebugFilePath) > 0 {
		if !writeVerifyDebugOutput(t, actualDebugFilePath, start, end, actual) {
			return false
		}
	}

	if !assert.Equal(t, len(expected), len(actual)) {
		return false
	}
	for i, series := range actual {
		if ts.ShouldBeEqual() {
			if !assert.Equal(t, expected[i], series) {
				return false
			}
		} else {
			assert.Equal(t, expected[i].ID, series.ID)
			if !ts.AssertEqual(t, expected[i].Data, series.Data) {
				return false
			}
		}
	}

	// Now check the metadata of all the series match
	ctx := context.NewBackground()
	defer ctx.Close()
	for _, shard := range ts.DB().ShardSet().AllIDs() {
		var (
			opts      block.FetchBlocksMetadataOptions
			pageToken storage.PageToken
			first     = true
		)
		for {
			if first {
				first = false
			} else if pageToken == nil {
				// Done, next shard
				break
			}

			results, nextPageToken, err := ts.DB().FetchBlocksMetadataV2(ctx,
				namespace, shard, start, end, 4096, pageToken, opts)
			assert.NoError(t, err)

			// Use the next one for the next iteration
			pageToken = nextPageToken

			for _, actual := range results.Results() {
				id := actual.ID.String()
				expected, ok := expectedMetadata[id]
				if !assert.True(t, ok, fmt.Sprintf("unexpected ID: %s", id)) {
					return false
				}

				expectedTagsIter := ident.NewTagsIterator(expected.Tags)
				actualTagsIter := actual.Tags.Duplicate()
				tagMatcher := ident.NewTagIterMatcher(expectedTagsIter)
				tagsMatch := tagMatcher.Matches(actualTagsIter)
				if !tagsMatch {
					expectedTagsIter.Reset(expected.Tags)
					actualTagsIter = actual.Tags.Duplicate()
					var expected, actual string
					for expectedTagsIter.Next() {
						tag := expectedTagsIter.Current()
						entry := ""
						if expected != "" {
							entry += ", "
						}
						entry += tag.Name.String() + "=" + tag.Value.String()
						expected += entry
					}
					for actualTagsIter.Next() {
						tag := actualTagsIter.Current()
						entry := ""
						if actual != "" {
							entry += " "
						}
						entry += tag.Name.String() + "=" + tag.Value.String()
						actual += entry
					}
					ts.StorageOpts().InstrumentOptions().Logger().
						Error("series does not match expected tags",
							zap.String("id", id),
							zap.String("expectedTags", expected),
							zap.String("actualTags", actual),
							zap.Any("expectedTagsErr", expectedTagsIter.Err()),
							zap.Any("actualTagsErrr", actualTagsIter.Err()),
						)
				}

				if !assert.True(t, tagMatcher.Matches(actualTagsIter)) {
					return false
				}
			}
		}
	}

	return true
}

func containsSeries(ts TestSetup, namespace, seriesID ident.ID, start, end time.Time) (bool, error) {
	req := rpc.NewFetchRequest()
	req.NameSpace = namespace.String()
	req.ID = seriesID.String()
	req.RangeStart = xtime.ToNormalizedTime(start, time.Second)
	req.RangeEnd = xtime.ToNormalizedTime(end, time.Second)
	req.ResultTimeType = rpc.TimeType_UNIX_SECONDS
	fetched, err := ts.Fetch(req)
	return len(fetched) != 0, err
}

func writeVerifyDebugOutput(
	t *testing.T, filePath string, start, end time.Time, series generate.SeriesBlock) bool {
	w, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if !assert.NoError(t, err) {
		return false
	}

	list := make(readableSeriesList, 0, len(series))
	for i := range series {
		tags := make([]readableSeriesTag, len(series[i].Tags.Values()))
		for _, tag := range series[i].Tags.Values() {
			tags = append(tags, readableSeriesTag{
				Name:  tag.Name.String(),
				Value: tag.Value.String(),
			})
		}
		list = append(list, readableSeries{
			ID:   series[i].ID.String(),
			Tags: tags,
			Data: series[i].Data,
		})
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
	if !assert.NoError(t, err) {
		return false
	}

	_, err = w.Write(data)
	if !assert.NoError(t, err) {
		return false
	}
	return assert.NoError(t, w.Close())
}

func verifySeriesMaps(
	t *testing.T,
	ts TestSetup,
	namespace ident.ID,
	seriesMaps map[xtime.UnixNano]generate.SeriesBlock,
) bool {
	debugFilePathPrefix := ts.Opts().VerifySeriesDebugFilePathPrefix()
	expectedDebugFilePath, ok := createFileIfPrefixSet(t, debugFilePathPrefix, fmt.Sprintf("%s-expected.log", namespace.String()))
	if !ok {
		return false
	}
	actualDebugFilePath, ok := createFileIfPrefixSet(t, debugFilePathPrefix, fmt.Sprintf("%s-actual.log", namespace.String()))
	if !ok {
		return false
	}

	nsMetadata, ok := ts.DB().Namespace(namespace)
	if !assert.True(t, ok) {
		return false
	}
	nsOpts := nsMetadata.Options()

	for timestamp, sm := range seriesMaps {
		start := timestamp.ToTime()
		end := start.Add(nsOpts.RetentionOptions().BlockSize())
		matches := verifySeriesMapForRange(
			t, ts, start, end, namespace, sm,
			expectedDebugFilePath, actualDebugFilePath)
		if !matches {
			return false
		}
	}

	return true
}

func createFileIfPrefixSet(t *testing.T, prefix, suffix string) (string, bool) {
	if len(prefix) == 0 {
		return "", true
	}
	filePath := prefix + "_" + suffix
	w, err := os.Create(filePath)
	if !assert.NoError(t, err) {
		return "", false
	}
	if !assert.NoError(t, w.Close()) {
		return "", false
	}
	return filePath, true
}

func compareSeriesList(
	expected generate.SeriesBlock,
	actual generate.SeriesBlock,
) error {
	sort.Sort(expected)
	sort.Sort(actual)

	if len(expected) != len(actual) {
		return fmt.Errorf(
			"number of expected series: %d did not match actual: %d",
			len(expected), len(actual))
	}

	for i := range expected {
		if !bytes.Equal(expected[i].ID.Bytes(), actual[i].ID.Bytes()) {
			return fmt.Errorf(
				"series ID did not match, expected: %s, actual: %s",
				expected[i].ID.String(), actual[i].ID.String())
		}
		if len(expected[i].Data) != len(actual[i].Data) {
			return fmt.Errorf(
				"data for series: %s did not match, expected: %d data points, actual: %d",
				expected[i].ID.String(), len(expected[i].Data), len(actual[i].Data))
		}
		if !reflect.DeepEqual(expected[i].Data, actual[i].Data) {
			return fmt.Errorf(
				"data for series: %s did not match, expected: %v, actual: %v",
				expected[i].ID.String(), expected[i].Data, actual[i].Data)
		}
	}

	return nil
}
