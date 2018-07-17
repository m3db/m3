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

	"github.com/m3db/m3db/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3db/src/dbnode/integration/generate"
	"github.com/m3db/m3db/src/dbnode/storage"
	"github.com/m3db/m3db/src/dbnode/storage/block"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

type readableSeries struct {
	ID   string
	Tags []readableSeriesTag
	Data []ts.Datapoint
}

type readableSeriesTag struct {
	Name  string
	Value string
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
	namespace ident.ID,
	input generate.SeriesBlock,
	expectedDebugFilePath string,
	actualDebugFilePath string,
) {
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
		fetched, err := ts.fetch(req)

		require.NoError(t, err)
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
		writeVerifyDebugOutput(t, expectedDebugFilePath, start, end, expected)
	}
	if len(actualDebugFilePath) > 0 {
		writeVerifyDebugOutput(t, actualDebugFilePath, start, end, actual)
	}

	for i, series := range actual {
		require.Equal(t, expected[i], series)
	}
	require.Equal(t, expected, actual)

	// Now check the metadata of all the series match
	ctx := context.NewContext()
	defer ctx.Close()
	for _, shard := range ts.db.ShardSet().AllIDs() {
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

			results, nextPageToken, err := ts.db.FetchBlocksMetadataV2(ctx,
				namespace, shard, start, end, 4096, pageToken, opts)
			require.NoError(t, err)

			// Use the next one for the next iteration
			pageToken = nextPageToken

			for _, actual := range results.Results() {
				id := actual.ID.String()
				expected, ok := expectedMetadata[id]
				require.True(t, ok, fmt.Sprintf("unexpected ID: %s", id))

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
					ts.logger.WithFields(
						xlog.NewField("id", id),
						xlog.NewField("expectedTags", expected),
						xlog.NewField("actualTags", actual),
					).Error("series does not match expected tags")
				}

				require.True(t, tagMatcher.Matches(actualTagsIter))
			}
		}
	}
}

func writeVerifyDebugOutput(t *testing.T, filePath string, start, end time.Time, series generate.SeriesBlock) {
	w, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	require.NoError(t, err)

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
	require.NoError(t, err)

	_, err = w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
}

func verifySeriesMaps(
	t *testing.T,
	ts *testSetup,
	namespace ident.ID,
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

func compareSeriesList(
	t *testing.T,
	expected generate.SeriesBlock,
	actual generate.SeriesBlock,
) {
	sort.Sort(expected)
	sort.Sort(actual)

	require.Equal(t, len(expected), len(actual))

	for i := range expected {
		require.Equal(t, expected[i].ID.Bytes(), actual[i].ID.Bytes())
		require.Equal(t, expected[i].Data, expected[i].Data)
	}
}
