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

	"github.com/m3db/m3coordinator/generated/proto/rpc"
	"github.com/m3db/m3coordinator/models"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/ts"

	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	name0    = "regex"
	val0     = "[a-z]"
	spec0    = "specs"
	valList0 = []float32{1.0, 2.0, 3.0}
	mps0     = int32(100)
	time0    = "2000-02-06T11:54:48+07:00"

	name1    = "eq"
	val1     = "val"
	spec1    = "fix"
	valList1 = []float32{4.0, 5.0, 6.0}
	mps1     = int32(120)
	time1    = "2093-02-06T11:54:48+07:00"

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

func createRPCSeries(t *testing.T) ([]*rpc.Series, time.Time, time.Time) {
	t0, t1 := parseTimes(t)
	return []*rpc.Series{
		&rpc.Series{
			Name:          name0,
			StartTime:     fromTime(t0),
			Values:        valList0,
			Tags:          tags0,
			Specification: spec0,
			MillisPerStep: mps0,
		},
		&rpc.Series{
			Name:          name1,
			StartTime:     fromTime(t1),
			Values:        valList1,
			Tags:          tags1,
			Specification: spec1,
			MillisPerStep: mps1,
		},
	}, t0, t1
}

func TestDecodeFetchResult(t *testing.T) {
	ctx := context.Background()
	rpcSeries, t0, t1 := createRPCSeries(t)

	tsSeries := DecodeFetchResult(ctx, rpcSeries)
	assert.Len(t, tsSeries, 2)
	assert.Equal(t, name0, tsSeries[0].Name())
	assert.Equal(t, name1, tsSeries[1].Name())
	assert.True(t, t0.Equal(tsSeries[0].StartTime()))
	assert.True(t, t1.Equal(tsSeries[1].StartTime()))
	assert.Equal(t, spec0, tsSeries[0].Specification)
	assert.Equal(t, spec1, tsSeries[1].Specification)
	assert.Equal(t, models.Tags(tags0), tsSeries[0].Tags)
	assert.Equal(t, models.Tags(tags1), tsSeries[1].Tags)

	assert.Equal(t, len(valList0), tsSeries[0].Len())
	assert.Equal(t, len(valList1), tsSeries[1].Len())
	assert.Equal(t, int(mps0), tsSeries[0].MillisPerStep())
	assert.Equal(t, int(mps1), tsSeries[1].MillisPerStep())
	for i := range valList0 {
		assert.Equal(t, float64(valList0[i]), tsSeries[0].ValueAt(i))
	}
	for i := range valList1 {
		assert.Equal(t, float64(valList1[i]), tsSeries[1].ValueAt(i))
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
	points := []*ts.Datapoint{
		&ts.Datapoint{
			Timestamp: t0,
			Value:     float0,
		},
		&ts.Datapoint{
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
		assert.Equal(t, float32(v.Value), encPoints[i].GetValue())
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
