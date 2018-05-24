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
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/generated/proto/rpc"
	"github.com/m3db/m3db/src/coordinator/models"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/ts"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	now      = time.Now()
	name0    = "regex"
	val0     = "[a-z]"
	valList0 = &rpc.Datapoints{
		Datapoints:      []*rpc.Datapoint{{1, 1.0}, {2, 2.0}, {3, 3.0}},
		FixedResolution: false}
	time0 = "2000-02-06T11:54:48+07:00"

	name1    = "eq"
	val1     = "val"
	valList1 = &rpc.Datapoints{
		Datapoints:      []*rpc.Datapoint{{1, 4.0}, {2, 5.0}, {3, 6.0}},
		FixedResolution: false}

	name2    = "s2"
	valList2 = &rpc.Datapoints{
		Datapoints:      []*rpc.Datapoint{{fromTime(now.Add(-3 * time.Minute)), 4.0}, {fromTime(now.Add(-2 * time.Minute)), 5.0}, {fromTime(now.Add(-1 * time.Minute)), 6.0}},
		FixedResolution: true}

	time1 = "2093-02-06T11:54:48+07:00"

	tags0  = map[string]string{"a": "b", "c": "d"}
	tags1  = map[string]string{"e": "f", "g": "h"}
	float0 = 100.0
	float1 = 3.5
	ann    = []byte("aasjgał")
	id     = "asdgsdh"
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

func createRPCSeries() []*rpc.Series {
	return []*rpc.Series{
		&rpc.Series{
			Name:   name0,
			Values: valList0,
			Tags:   tags0,
		},
		&rpc.Series{
			Name:   name1,
			Values: valList1,
			Tags:   tags1,
		},
		&rpc.Series{
			Name:   name2,
			Values: valList2,
			Tags:   tags1,
		},
	}
}

func TestDecodeFetchResult(t *testing.T) {
	ctx := context.Background()
	rpcSeries := createRPCSeries()

	tsSeries, err := DecodeFetchResult(ctx, rpcSeries)
	assert.NoError(t, err)
	assert.Len(t, tsSeries, 3)
	assert.Equal(t, name0, tsSeries[0].Name())
	assert.Equal(t, name1, tsSeries[1].Name())
	assert.Equal(t, models.Tags(tags0), tsSeries[0].Tags)
	assert.Equal(t, models.Tags(tags1), tsSeries[1].Tags)

	assert.Equal(t, len(valList0.Datapoints), tsSeries[0].Len())
	assert.Equal(t, len(valList1.Datapoints), tsSeries[1].Len())
	assert.Equal(t, len(valList2.Datapoints), tsSeries[2].Len())

	for i := range valList0.Datapoints {
		assert.Equal(t, float64(valList0.Datapoints[i].Value), tsSeries[0].Values().ValueAt(i))
		assert.Equal(t, valList0.Datapoints[i].Timestamp, fromTime(tsSeries[0].Values().DatapointAt(i).Timestamp))
	}
	for i := range valList1.Datapoints {
		assert.Equal(t, float64(valList1.Datapoints[i].Value), tsSeries[1].Values().ValueAt(i))
		assert.Equal(t, valList1.Datapoints[i].Timestamp, fromTime(tsSeries[1].Values().DatapointAt(i).Timestamp))
	}
	for i := range valList2.Datapoints {
		assert.Equal(t, float64(valList2.Datapoints[i].Value), tsSeries[2].Values().ValueAt(i))
		assert.Equal(t, valList2.Datapoints[i].Timestamp, fromTime(tsSeries[2].Values().DatapointAt(i).Timestamp))
	}

	// Encode again

	fetchResult := &storage.FetchResult{SeriesList: tsSeries}
	revert := EncodeFetchResult(fetchResult)
	assert.Equal(t, rpcSeries, revert.GetSeries())
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

	matchers := []*models.Matcher{m0, m1}
	return &storage.FetchQuery{
		TagMatchers: matchers,
		Start:       start,
		End:         end,
	}, start, end
}

func TestEncodeFetchMessage(t *testing.T) {
	rQ, start, end := createStorageFetchQuery(t)

	grpcQ := EncodeFetchMessage(rQ, id)
	require.NotNil(t, grpcQ)
	assert.Equal(t, fromTime(start), grpcQ.GetQuery().GetStart())
	assert.Equal(t, fromTime(end), grpcQ.GetQuery().GetEnd())
	mRPC := grpcQ.GetQuery().GetTagMatchers()
	assert.Equal(t, 2, len(mRPC))
	assert.Equal(t, name0, mRPC[0].GetName())
	assert.Equal(t, val0, mRPC[0].GetValue())
	assert.Equal(t, models.MatchRegexp, models.MatchType(mRPC[0].GetType()))
	assert.Equal(t, name1, mRPC[1].GetName())
	assert.Equal(t, val1, mRPC[1].GetValue())
	assert.Equal(t, models.MatchEqual, models.MatchType(mRPC[1].GetType()))
	assert.Equal(t, id, grpcQ.GetOptions().GetId())
}

func TestEncodeDecodeFetchQuery(t *testing.T) {
	rQ, _, _ := createStorageFetchQuery(t)
	gq := EncodeFetchMessage(rQ, id)
	reverted, decodeID, err := DecodeFetchMessage(gq)
	require.Nil(t, err)
	assert.Equal(t, id, decodeID)
	readQueriesAreEqual(t, rQ, reverted)

	// Encode again
	gqr := EncodeFetchMessage(reverted, decodeID)
	assert.Equal(t, gq, gqr)
}

func createStorageWriteQuery(t *testing.T) (*storage.WriteQuery, ts.Datapoints) {
	t0, t1 := parseTimes(t)
	points := []ts.Datapoint{
		ts.Datapoint{
			Timestamp: t0,
			Value:     float0,
		},
		ts.Datapoint{
			Timestamp: t1,
			Value:     float1,
		},
	}
	return &storage.WriteQuery{
		Tags:       tags0,
		Unit:       xtime.Unit(2),
		Annotation: ann,
		Datapoints: points,
	}, points
}

func TestEncodeWriteMessage(t *testing.T) {
	write, points := createStorageWriteQuery(t)
	encw := EncodeWriteMessage(write, id)
	assert.Equal(t, tags0, encw.GetQuery().GetTags())
	assert.Equal(t, ann, encw.GetQuery().GetAnnotation())
	assert.Equal(t, int32(2), encw.GetQuery().GetUnit())
	assert.Equal(t, id, encw.GetOptions().GetId())
	encPoints := encw.GetQuery().GetDatapoints()
	assert.Equal(t, len(points), len(encPoints))
	for i, v := range points {
		assert.Equal(t, fromTime(v.Timestamp), encPoints[i].GetTimestamp())
		assert.Equal(t, v.Value, encPoints[i].GetValue())
	}
}

func writeQueriesAreEqual(t *testing.T, this, other *storage.WriteQuery) {
	assert.Equal(t, this.Annotation, other.Annotation)
	assert.Equal(t, this.Tags, other.Tags)
	assert.Equal(t, this.Unit, other.Unit)
	assert.Equal(t, this.Datapoints.Len(), other.Datapoints.Len())
	for i := 0; i < this.Datapoints.Len(); i++ {
		assert.Equal(t, this.Datapoints.ValueAt(i), other.Datapoints.ValueAt(i))
		assert.True(t, this.Datapoints[i].Timestamp.Equal(other.Datapoints[i].Timestamp))
	}
}

func TestEncodeDecodeWriteQuery(t *testing.T) {
	write, _ := createStorageWriteQuery(t)
	encw := EncodeWriteMessage(write, id)
	rev, decodeID := DecodeWriteMessage(encw)
	writeQueriesAreEqual(t, write, rev)
	require.Equal(t, id, decodeID)

	// Encode again
	reencw := EncodeWriteMessage(rev, decodeID)
	assert.Equal(t, encw, reencw)
}
