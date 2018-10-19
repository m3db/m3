// Copyright (c) 2018 Uber Technologies, Inc.
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

package remote

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

var (
	now      = time.Now()
	name0    = []byte("regex")
	val0     = []byte("[a-z]")
	valList0 = []*rpc.Datapoint{{1, 1.0}, {2, 2.0}, {3, 3.0}}
	time0    = "2000-02-06T11:54:48+07:00"

	name1    = []byte("eq")
	val1     = []byte("val")
	valList1 = []*rpc.Datapoint{{1, 4.0}, {2, 5.0}, {3, 6.0}}

	valList2 = []*rpc.Datapoint{
		{fromTime(now.Add(-3 * time.Minute)), 4.0},
		{fromTime(now.Add(-2 * time.Minute)), 5.0},
		{fromTime(now.Add(-1 * time.Minute)), 6.0},
	}

	time1 = "2093-02-06T11:54:48+07:00"

	tags0 = test.StringTagsToTags(test.StringTags{{N: "a", V: "b"}, {N: "c", V: "d"}})
	tags1 = test.StringTagsToTags(test.StringTags{{N: "e", V: "f"}, {N: "g", V: "h"}})
)

func parseTimes(t *testing.T) (time.Time, time.Time) {
	t0, err := time.Parse(time.RFC3339, time0)
	require.Nil(t, err)
	t1, err := time.Parse(time.RFC3339, time1)
	require.Nil(t, err)
	return t0, t1
}

func TestTimeConversions(t *testing.T) {
	time, _ := parseTimes(t)
	tix := fromTime(time)
	assert.True(t, time.Equal(toTime(tix)))
	assert.Equal(t, tix, fromTime(toTime(tix)))
}

func createRPCSeries() []*rpc.DecompressedSeries {
	return []*rpc.DecompressedSeries{
		&rpc.DecompressedSeries{
			Datapoints: valList0,
			Tags:       encodeTags(tags0),
		},
		&rpc.DecompressedSeries{
			Datapoints: valList1,
			Tags:       encodeTags(tags1),
		},
		&rpc.DecompressedSeries{
			Datapoints: valList2,
			Tags:       encodeTags(tags1),
		},
	}
}

func TestDecodeFetchResult(t *testing.T) {
	rpcSeries := createRPCSeries()
	name := "name"
	metricName := []byte("!")

	tsSeries, err := decodeDecompressedFetchResult(name, models.NewTagOptions().SetMetricName(metricName), rpcSeries)
	assert.NoError(t, err)
	assert.Len(t, tsSeries, 3)
	assert.Equal(t, name, tsSeries[0].Name())
	assert.Equal(t, name, tsSeries[1].Name())
	assert.Equal(t, tags0.Tags, tsSeries[0].Tags.Tags)
	assert.Equal(t, metricName, tsSeries[0].Tags.Opts.MetricName())
	assert.Equal(t, tags1.Tags, tsSeries[1].Tags.Tags)
	assert.Equal(t, metricName, tsSeries[1].Tags.Opts.MetricName())

	assert.Equal(t, len(valList0), tsSeries[0].Len())
	assert.Equal(t, len(valList1), tsSeries[1].Len())
	assert.Equal(t, len(valList2), tsSeries[2].Len())

	for i := range valList0 {
		assert.Equal(t, float64(valList0[i].Value), tsSeries[0].Values().ValueAt(i))
		assert.Equal(t, valList0[i].Timestamp, fromTime(tsSeries[0].Values().DatapointAt(i).Timestamp))
	}
	for i := range valList1 {
		assert.Equal(t, float64(valList1[i].Value), tsSeries[1].Values().ValueAt(i))
		assert.Equal(t, valList1[i].Timestamp, fromTime(tsSeries[1].Values().DatapointAt(i).Timestamp))
	}
	for i := range valList2 {
		assert.Equal(t, float64(valList2[i].Value), tsSeries[2].Values().ValueAt(i))
		assert.Equal(t, valList2[i].Timestamp, fromTime(tsSeries[2].Values().DatapointAt(i).Timestamp))
	}

	// Encode again
	fetchResult := &storage.FetchResult{SeriesList: tsSeries}
	revert := encodeFetchResult(fetchResult)
	require.Len(t, revert.GetSeries(), len(rpcSeries))
	for i, expected := range rpcSeries {
		assert.Equal(t, expected.GetDatapoints(), revert.GetSeries()[i].GetDecompressed().GetDatapoints())
		for _, tag := range expected.GetTags() {
			assert.Contains(t, revert.GetSeries()[i].GetDecompressed().GetTags(), tag)
		}
	}
}

func readQueriesAreEqual(t *testing.T, this, other *storage.FetchQuery) {
	assert.True(t, this.Start.Equal(other.Start))
	assert.True(t, this.End.Equal(other.End))
	assert.Equal(t, len(this.TagMatchers), len(other.TagMatchers))
	assert.Equal(t, 2, len(other.TagMatchers))
	for i, matcher := range this.TagMatchers {
		assert.Equal(t, matcher.Type, other.TagMatchers[i].Type)
		assert.Equal(t, matcher.Name, other.TagMatchers[i].Name)
		assert.Equal(t, matcher.Value, other.TagMatchers[i].Value)
	}
}

func createStorageFetchQuery(t *testing.T) (*storage.FetchQuery, time.Time, time.Time) {
	m0, err := models.NewMatcher(models.MatchRegexp, name0, val0)
	require.Nil(t, err)
	m1, err := models.NewMatcher(models.MatchEqual, name1, val1)
	require.Nil(t, err)
	start, end := parseTimes(t)

	matchers := []models.Matcher{m0, m1}
	return &storage.FetchQuery{
		TagMatchers: matchers,
		Start:       start,
		End:         end,
	}, start, end
}

func TestEncodeFetchMessage(t *testing.T) {
	rQ, start, end := createStorageFetchQuery(t)

	grpcQ, err := encodeFetchRequest(rQ)
	require.NotNil(t, grpcQ)
	require.NoError(t, err)
	assert.Equal(t, fromTime(start), grpcQ.GetStart())
	assert.Equal(t, fromTime(end), grpcQ.GetEnd())
	mRPC := grpcQ.GetTagMatchers().GetTagMatchers()
	assert.Equal(t, 2, len(mRPC))
	assert.Equal(t, name0, mRPC[0].GetName())
	assert.Equal(t, val0, mRPC[0].GetValue())
	assert.Equal(t, models.MatchRegexp, models.MatchType(mRPC[0].GetType()))
	assert.Equal(t, name1, mRPC[1].GetName())
	assert.Equal(t, val1, mRPC[1].GetValue())
	assert.Equal(t, models.MatchEqual, models.MatchType(mRPC[1].GetType()))
}

func TestEncodeDecodeFetchQuery(t *testing.T) {
	rQ, _, _ := createStorageFetchQuery(t)
	gq, err := encodeFetchRequest(rQ)
	require.NoError(t, err)
	reverted, err := decodeFetchRequest(gq)
	require.NoError(t, err)
	readQueriesAreEqual(t, rQ, reverted)

	// Encode again
	gqr, err := encodeFetchRequest(reverted)
	require.NoError(t, err)
	assert.Equal(t, gq, gqr)
}

func TestencodeMetadata(t *testing.T) {
	headers := make(http.Header)
	headers.Add("Foo", "bar")
	headers.Add("Foo", "baz")
	headers.Add("Foo", "abc")
	headers.Add("lorem", "ipsum")
	ctx := context.WithValue(context.TODO(), handler.HeaderKey, headers)
	requestID := "requestID"

	encodedCtx := encodeMetadata(ctx, requestID)
	md, ok := metadata.FromOutgoingContext(encodedCtx)
	require.True(t, ok)
	assert.Equal(t, []string{"bar", "baz", "abc"}, md["foo"], "metadat keys must be lower case")
	assert.Equal(t, []string{"ipsum"}, md["lorem"])
	assert.Equal(t, []string{requestID}, md[reqIDKey])
}

func TestRetrieveMetadata(t *testing.T) {
	logging.InitWithCores(nil)

	headers := make(http.Header)
	headers.Add("Foo", "bar")
	headers.Add("Foo", "baz")
	headers.Add("Foo", "abc")
	headers.Add("Lorem", "ipsum")
	requestID := "requestID"
	headers[reqIDKey] = []string{requestID}
	ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD(headers))
	encodedCtx := retrieveMetadata(ctx)

	require.Equal(t, requestID, logging.ReadContextID(encodedCtx))
}
