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

package node

import (
	"bytes"
	gocontext "context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/convert"
	tterrors "github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/errors"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/tracepoint"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/ts/writes"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/serialize"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/thrift"
)

// Create opts once to avoid recreating a lot of default pools, etc
var (
	testIndexOptions          = index.NewOptions()
	testNamespaceOptions      = namespace.NewOptions()
	testStorageOpts           = storage.NewOptions()
	testTChannelThriftOptions = tchannelthrift.NewOptions()
)

func init() {
	// Set all pool sizes to 1 for tests
	segmentArrayPoolSize = 1
	writeBatchPooledReqPoolSize = 1
}

func TestServiceHealth(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	// Assert bootstrapped false
	mockDB.EXPECT().IsBootstrappedAndDurable().Return(false)

	tctx, _ := thrift.NewContext(time.Minute)
	result, err := service.Health(tctx)
	require.NoError(t, err)

	assert.Equal(t, true, result.Ok)
	assert.Equal(t, "up", result.Status)
	assert.Equal(t, false, result.Bootstrapped)

	// Assert bootstrapped true
	mockDB.EXPECT().IsBootstrappedAndDurable().Return(true)

	tctx, _ = thrift.NewContext(time.Minute)
	result, err = service.Health(tctx)
	require.NoError(t, err)

	assert.Equal(t, true, result.Ok)
	assert.Equal(t, "up", result.Status)
	assert.Equal(t, true, result.Bootstrapped)
}

func TestServiceBootstrapped(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	// Should return an error when not bootstrapped
	mockDB.EXPECT().IsBootstrappedAndDurable().Return(false)
	tctx, _ := thrift.NewContext(time.Minute)
	_, err := service.Bootstrapped(tctx)
	require.Error(t, err)

	// Should not return an error when bootstrapped
	mockDB.EXPECT().IsBootstrappedAndDurable().Return(true)

	tctx, _ = thrift.NewContext(time.Minute)
	_, err = service.Health(tctx)
	require.NoError(t, err)
}

func TestServiceBootstrappedInPlacementOrNoPlacement(t *testing.T) {
	type TopologyIsSetResult struct {
		result bool
		err    error
	}

	type bootstrappedAndDurableResult struct {
		result bool
	}

	tests := []struct {
		name                   string
		dbSet                  bool
		TopologyIsSet          *TopologyIsSetResult
		bootstrappedAndDurable *bootstrappedAndDurableResult
		expectErr              bool
	}{
		{
			name:                   "bootstrapped in placement",
			dbSet:                  true,
			TopologyIsSet:          &TopologyIsSetResult{result: true, err: nil},
			bootstrappedAndDurable: &bootstrappedAndDurableResult{result: true},
		},
		{
			name:          "not in placement",
			dbSet:         true,
			TopologyIsSet: &TopologyIsSetResult{result: false, err: nil},
		},
		{
			name:          "topology check error",
			dbSet:         true,
			TopologyIsSet: &TopologyIsSetResult{result: false, err: errors.New("an error")},
			expectErr:     true,
		},
		{
			name:          "db not set in placement",
			dbSet:         false,
			TopologyIsSet: &TopologyIsSetResult{result: true, err: nil},
			expectErr:     true,
		},
		{
			name:                   "not bootstrapped in placement",
			dbSet:                  true,
			TopologyIsSet:          &TopologyIsSetResult{result: true, err: nil},
			bootstrappedAndDurable: &bootstrappedAndDurableResult{result: false},
			expectErr:              true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := xtest.NewController(t)
			defer ctrl.Finish()

			// Simulate placement
			mockTopoInit := topology.NewMockInitializer(ctrl)
			if r := test.TopologyIsSet; r != nil {
				mockTopoInit.EXPECT().TopologyIsSet().Return(r.result, r.err)
			}

			var db storage.Database
			if test.dbSet {
				mockDB := storage.NewMockDatabase(ctrl)
				mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
				// Simulate bootstrapped and durable
				if r := test.bootstrappedAndDurable; r != nil {
					mockDB.EXPECT().IsBootstrappedAndDurable().Return(r.result)
				}
				db = mockDB
			}

			testOpts := testTChannelThriftOptions.
				SetTopologyInitializer(mockTopoInit)
			service := NewService(db, testOpts).(*service)

			// Call BootstrappedInPlacementOrNoPlacement
			tctx, _ := thrift.NewContext(time.Minute)
			_, err := service.BootstrappedInPlacementOrNoPlacement(tctx)
			if test.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestServiceQuery(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	enc := testStorageOpts.EncoderPool().Get()
	enc.Reset(start, 0, nil)

	nsID := "metrics"

	streams := map[string]xio.SegmentReader{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	tags := map[string][]struct {
		name  string
		value string
	}{
		"foo": {{"foo", "bar"}, {"baz", "dxk"}},
		"bar": {{"foo", "bar"}, {"dzk", "baz"}},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0, nil)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		stream, _ := enc.Stream(ctx)
		streams[id] = stream
		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return([][]xio.BlockReader{{
				xio.BlockReader{
					SegmentReader: stream,
				},
			}}, nil)
	}

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	md1 := doc.Metadata{
		ID: ident.BytesID("foo"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name:  []byte("baz"),
				Value: []byte("dxk"),
			},
		},
	}
	md2 := doc.Metadata{
		ID: ident.BytesID("bar"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name:  []byte("dzk"),
				Value: []byte("baz"),
			},
		},
	}

	resMap := index.NewQueryResults(ident.StringID(nsID),
		index.QueryResultsOptions{}, testIndexOptions)
	resMap.Map().Set(md1.ID, doc.NewDocumentFromMetadata(md1))
	resMap.Map().Set(md2.ID, doc.NewDocumentFromMetadata(md2))

	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			SeriesLimit:    10,
		}).Return(index.QueryResult{Results: resMap, Exhaustive: true}, nil)

	limit := int64(10)
	r, err := service.Query(tctx, &rpc.QueryRequest{
		Query: &rpc.Query{
			Regexp: &rpc.RegexpQuery{
				Field:  "foo",
				Regexp: "b.*",
			},
		},
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		Limit:          &limit,
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Results, func(i, j int) bool {
		return r.Results[i].ID < r.Results[j].ID
	})

	ids := []string{"bar", "foo"}
	require.Equal(t, len(ids), len(r.Results))
	for i, id := range ids {
		elem := r.Results[i]
		require.NotNil(t, elem)

		require.Equal(t, elem.ID, id)
		require.Equal(t, len(tags[id]), len(elem.Tags))
		for i, tag := range elem.Tags {
			assert.Equal(t, tags[id][i].name, tag.Name)
			assert.Equal(t, tags[id][i].value, tag.Value)
		}

		require.Equal(t, len(series[id]), len(elem.Datapoints))
		for i, dp := range elem.Datapoints {
			assert.Equal(t, series[id][i].t.Unix(), dp.Timestamp)
			assert.Equal(t, series[id][i].v, dp.Value)
		}
	}
}

func TestServiceSetMetadata(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	size := 100
	mockDB := storage.NewMockDatabase(ctrl)
	service := NewService(mockDB, testTChannelThriftOptions).(*service)
	metas := make([]string, 0, size)
	for i := 0; i < size; i++ {
		metas = append(metas, fmt.Sprint(i))
	}

	var wg sync.WaitGroup
	for _, md := range metas {
		wg.Add(1)
		md := md
		go func() {
			service.SetMetadata(md, md)
			wg.Done()
		}()
	}

	wg.Wait()
	for _, md := range metas {
		wg.Add(1)
		md := md
		go func() {
			meta, ok := service.Metadata(md)
			assert.True(t, ok)
			assert.Equal(t, meta, md)
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestServiceQueryOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
		limit   = int64(100)
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0, nil)

	_, err := service.Query(tctx, &rpc.QueryRequest{
		Query: &rpc.Query{
			Regexp: &rpc.RegexpQuery{
				Field:  "foo",
				Regexp: "b.*",
			},
		},
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		Limit:          &limit,
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceQueryDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
		limit   = int64(100)
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0, nil)

	_, err := service.Query(tctx, &rpc.QueryRequest{
		Query: &rpc.Query{
			Regexp: &rpc.RegexpQuery{
				Field:  "foo",
				Regexp: "b.*",
			},
		},
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		Limit:          &limit,
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceQueryUnknownErr(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc := testStorageOpts.EncoderPool().Get()
	enc.Reset(start, 0, nil)

	nsID := "metrics"
	unknownErr := fmt.Errorf("unknown-error")
	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			SeriesLimit:    10,
		}).Return(index.QueryResult{}, unknownErr)

	limit := int64(10)
	_, err = service.Query(tctx, &rpc.QueryRequest{
		Query: &rpc.Query{
			Regexp: &rpc.RegexpQuery{
				Field:  "foo",
				Regexp: "b.*",
			},
		},
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		Limit:          &limit,
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Error(t, err)
	require.Equal(t, convert.ToRPCError(unknownErr), err)
}

func TestServiceFetch(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	enc := testStorageOpts.EncoderPool().Get()
	enc.Reset(start, 0, nil)

	nsID := "metrics"

	values := []struct {
		t time.Time
		v float64
	}{
		{start.Add(10 * time.Second), 1.0},
		{start.Add(20 * time.Second), 2.0},
	}
	for _, v := range values {
		dp := ts.Datapoint{
			Timestamp: v.t,
			Value:     v.v,
		}
		require.NoError(t, enc.Encode(dp, xtime.Second, nil))
	}

	stream, _ := enc.Stream(ctx)
	mockDB.EXPECT().
		ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher("foo"), start, end).
		Return([][]xio.BlockReader{
			{
				{
					SegmentReader: stream,
				},
			},
		}, nil)

	r, err := service.Fetch(tctx, &rpc.FetchRequest{
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		ID:             "foo",
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.NoError(t, err)

	require.Equal(t, len(values), len(r.Datapoints))
	for i, v := range values {
		assert.Equal(t, v.t, time.Unix(r.Datapoints[i].Timestamp, 0))
		assert.Equal(t, v.v, r.Datapoints[i].Value)
	}
}

func TestServiceFetchIsOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0, nil)

	_, err := service.Fetch(tctx, &rpc.FetchRequest{
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		ID:             "foo",
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0, nil)

	_, err := service.Fetch(tctx, &rpc.FetchRequest{
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		ID:             "foo",
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceFetchUnknownErr(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)
	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	nsID := "metrics"
	unknownErr := fmt.Errorf("unknown-err")

	mockDB.EXPECT().
		ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher("foo"), start, end).
		Return(nil, unknownErr)

	_, err := service.Fetch(tctx, &rpc.FetchRequest{
		RangeStart:     start.Unix(),
		RangeEnd:       end.Unix(),
		RangeType:      rpc.TimeType_UNIX_SECONDS,
		NameSpace:      nsID,
		ID:             "foo",
		ResultTimeType: rpc.TimeType_UNIX_SECONDS,
	})
	require.Error(t, err)
	require.Equal(t, convert.ToRPCError(unknownErr), err)
}

func TestServiceFetchBatchRaw(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	nsID := "metrics"

	streams := map[string]xio.SegmentReader{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0, nil)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		stream, _ := enc.Stream(ctx)
		streams[id] = stream
		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return([][]xio.BlockReader{
				{
					{
						SegmentReader: stream,
					},
				},
			}, nil)
	}

	ids := [][]byte{[]byte("foo"), []byte("bar")}
	r, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart:    start.Unix(),
		RangeEnd:      end.Unix(),
		RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		NameSpace:     []byte(nsID),
		Ids:           ids,
	})
	require.NoError(t, err)

	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)

		assert.Nil(t, elem.Err)
		require.Equal(t, 1, len(elem.Segments))

		seg := elem.Segments[0]
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment, err := streams[string(id)].Segment()
		require.NoError(t, err)

		if expectSegment.Head != nil {
			expectHead = expectSegment.Head.Bytes()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Bytes()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

func TestServiceFetchBatchRawV2MultiNS(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	nsID1 := "metrics1"
	nsID2 := "metrics2"

	streams := map[string]xio.SegmentReader{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0, nil)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		stream, _ := enc.Stream(ctx)
		streams[id] = stream
		nsID := nsID1
		if id == "bar" {
			nsID = nsID2
		}
		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return([][]xio.BlockReader{
				{
					{
						SegmentReader: stream,
					},
				},
			}, nil)
	}

	ids := [][]byte{[]byte("foo"), []byte("bar")}
	elements := []*rpc.FetchBatchRawV2RequestElement{
		{
			NameSpace:     0,
			RangeStart:    start.Unix(),
			RangeEnd:      end.Unix(),
			ID:            []byte("foo"),
			RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		},
		{
			NameSpace:     1,
			RangeStart:    start.Unix(),
			RangeEnd:      end.Unix(),
			ID:            []byte("bar"),
			RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		},
	}
	r, err := service.FetchBatchRawV2(tctx, &rpc.FetchBatchRawV2Request{
		NameSpaces: [][]byte{[]byte(nsID1), []byte(nsID2)},
		Elements:   elements,
	})
	require.NoError(t, err)

	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)

		assert.Nil(t, elem.Err)
		require.Equal(t, 1, len(elem.Segments))

		seg := elem.Segments[0]
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment, err := streams[string(id)].Segment()
		require.NoError(t, err)

		if expectSegment.Head != nil {
			expectHead = expectSegment.Head.Bytes()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Bytes()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

// TestServiceFetchBatchRawOverMaxOutstandingRequests tests that the FetchBatchRaw endpoint
// will reject requests if the number of outstanding read requests has hit the maximum.
func TestServiceFetchBatchRawOverMaxOutstandingRequests(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	tchanOpts := testTChannelThriftOptions.
		SetMaxOutstandingReadRequests(1)
	service := NewService(mockDB, tchanOpts).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	var (
		nsID    = "metrics"
		streams = map[string]xio.SegmentReader{}
		series  = map[string][]struct {
			t time.Time
			v float64
		}{
			"foo": {
				{start.Add(10 * time.Second), 1.0},
				{start.Add(20 * time.Second), 2.0},
			},
		}

		requestIsOutstanding = make(chan struct{}, 0)
		testIsComplete       = make(chan struct{}, 0)
	)
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0, nil)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		stream, _ := enc.Stream(ctx)
		streams[id] = stream
		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Do(func(ctx interface{}, nsID ident.ID, seriesID ident.ID, start time.Time, end time.Time) {
				close(requestIsOutstanding)
				<-testIsComplete
			}).
			Return([][]xio.BlockReader{
				{
					{
						SegmentReader: stream,
					},
				},
			}, nil)
	}

	var (
		ids                          = [][]byte{[]byte("foo")}
		outstandingRequestIsComplete = make(chan struct{}, 0)
	)
	// First request will hang until the test is over simulating an "outstanding" request.
	go func() {
		service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
			RangeStart:    start.Unix(),
			RangeEnd:      end.Unix(),
			RangeTimeType: rpc.TimeType_UNIX_SECONDS,
			NameSpace:     []byte(nsID),
			Ids:           ids,
		})
		close(outstandingRequestIsComplete)
	}()

	<-requestIsOutstanding
	_, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart:    start.Unix(),
		RangeEnd:      end.Unix(),
		RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		NameSpace:     []byte(nsID),
		Ids:           ids,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
	close(testIsComplete)

	// Ensure the number of outstanding requests gets decremented at the end of the R.P.C.
	<-outstandingRequestIsComplete
	require.Equal(t, 0, service.state.numOutstandingReadRPCs)
}

func TestServiceFetchBatchRawUnknownError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)
	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	nsID := "metrics"
	unknownErr := fmt.Errorf("unknown-err")

	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
	}
	for id := range series {
		mockDB.EXPECT().
			ReadEncoded(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return(nil, unknownErr)
	}

	ids := [][]byte{[]byte("foo")}
	r, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart:    start.Unix(),
		RangeEnd:      end.Unix(),
		RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		NameSpace:     []byte(nsID),
		Ids:           ids,
	})
	require.NoError(t, err)
	require.Len(t, r.Elements, 1)
	require.Equal(t, convert.ToRPCError(unknownErr), r.Elements[0].Err)
}

func TestServiceFetchBatchRawIsOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		nsID    = "metrics"
		ids     = [][]byte{[]byte("foo"), []byte("bar")}
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0, nil)

	_, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart:    start.Unix(),
		RangeEnd:      end.Unix(),
		RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		NameSpace:     []byte(nsID),
		Ids:           ids,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchBatchRawDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		nsID    = "metrics"
		ids     = [][]byte{[]byte("foo"), []byte("bar")}
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	_, err := service.FetchBatchRaw(tctx, &rpc.FetchBatchRawRequest{
		RangeStart:    start.Unix(),
		RangeEnd:      end.Unix(),
		RangeTimeType: rpc.TimeType_UNIX_SECONDS,
		NameSpace:     []byte(nsID),
		Ids:           ids,
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceFetchBlocksRaw(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	nsID := "metrics"
	mockNs := storage.NewMockNamespace(ctrl)
	mockNs.EXPECT().Options().Return(testNamespaceOptions).AnyTimes()
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Namespace(ident.NewIDMatcher(nsID)).Return(mockNs, true).AnyTimes()
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	starts := []time.Time{start}

	streams := map[string]xio.SegmentReader{}
	checksums := map[string]uint32{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0, nil)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		stream, _ := enc.Stream(ctx)
		streams[id] = stream

		seg, err := streams[id].Segment()
		require.NoError(t, err)

		checksums[id] = seg.CalculateChecksum()
		expectedBlockReader := []xio.BlockReader{
			{
				SegmentReader: stream,
				Start:         start,
			},
		}

		mockDB.EXPECT().
			FetchBlocks(ctx, ident.NewIDMatcher(nsID), uint32(0), ident.NewIDMatcher(id), starts).
			Return([]block.FetchBlockResult{
				block.NewFetchBlockResult(start, expectedBlockReader, nil),
			}, nil)
	}

	ids := [][]byte{[]byte("foo"), []byte("bar")}
	r, err := service.FetchBlocksRaw(tctx, &rpc.FetchBlocksRawRequest{
		NameSpace: []byte(nsID),
		Shard:     0,
		Elements: []*rpc.FetchBlocksRawRequestElement{
			{
				ID:     ids[0],
				Starts: []int64{start.UnixNano()},
			},
			{
				ID:     ids[1],
				Starts: []int64{start.UnixNano()},
			},
		},
	})
	require.NoError(t, err)

	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)
		assert.Equal(t, id, elem.ID)

		require.Equal(t, 1, len(elem.Blocks))
		require.Nil(t, elem.Blocks[0].Err)
		require.Equal(t, checksums[string(id)], uint32(*(elem.Blocks[0].Checksum)))

		seg := elem.Blocks[0].Segments
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment, err := streams[string(id)].Segment()
		require.NoError(t, err)

		if expectSegment.Head != nil {
			expectHead = expectSegment.Head.Bytes()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Bytes()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}
}

func TestServiceFetchBlocksRawIsOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	nsID := "metrics"
	mockNs := storage.NewMockNamespace(ctrl)
	mockNs.EXPECT().Options().Return(testNamespaceOptions).AnyTimes()
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Namespace(ident.NewIDMatcher(nsID)).Return(mockNs, true).AnyTimes()
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		ids     = [][]byte{[]byte("foo"), []byte("bar")}
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0, nil)

	_, err := service.FetchBlocksRaw(tctx, &rpc.FetchBlocksRawRequest{
		NameSpace: []byte(nsID),
		Shard:     0,
		Elements: []*rpc.FetchBlocksRawRequestElement{
			{
				ID:     ids[0],
				Starts: []int64{start.UnixNano()},
			},
			{
				ID:     ids[1],
				Starts: []int64{start.UnixNano()},
			},
		},
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchBlocksRawDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		nsID    = "metrics"
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
		start   = time.Now().Add(-2 * time.Hour)
		end     = start.Add(2 * time.Hour)
		enc     = testStorageOpts.EncoderPool().Get()
		ids     = [][]byte{[]byte("foo"), []byte("bar")}
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	enc.Reset(start, 0, nil)

	_, err := service.FetchBlocksRaw(tctx, &rpc.FetchBlocksRawRequest{
		NameSpace: []byte(nsID),
		Shard:     0,
		Elements: []*rpc.FetchBlocksRawRequestElement{
			{
				ID:     ids[0],
				Starts: []int64{start.UnixNano()},
			},
			{
				ID:     ids[1],
				Starts: []int64{start.UnixNano()},
			},
		},
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceFetchBlocksMetadataEndpointV2Raw(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Setup mock db / service / context
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)
	service := NewService(mockDB, testTChannelThriftOptions).(*service)
	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	// Configure constants / options
	now := time.Now()
	var (
		start              = now.Truncate(time.Hour)
		end                = now.Add(4 * time.Hour).Truncate(time.Hour)
		limit              = int64(2)
		nextPageTokenBytes = []byte("page_next")
		includeSizes       = true
		includeChecksums   = true
		includeLastRead    = true
		nsID               = "metrics"
	)

	// Prepare test data
	type testBlock struct {
		start    time.Time
		size     int64
		checksum uint32
		lastRead time.Time
	}
	series := map[string]struct {
		tags ident.Tags
		data []testBlock
	}{
		"foo": {
			// Check with tags
			tags: ident.NewTags(
				ident.StringTag("aaa", "bbb"),
				ident.StringTag("ccc", "ddd"),
			),
			data: []testBlock{
				{start.Add(0 * time.Hour), 16, 111, time.Now().Add(-time.Minute)},
				{start.Add(2 * time.Hour), 32, 222, time.Time{}},
			},
		},
		"bar": {
			// And without tags
			tags: ident.Tags{},
			data: []testBlock{
				{start.Add(0 * time.Hour), 32, 222, time.Time{}},
				{start.Add(2 * time.Hour), 64, 333, time.Now().Add(-time.Minute)},
			},
		},
	}
	ids := make([][]byte, 0, len(series))
	mockResult := block.NewFetchBlocksMetadataResults()
	numBlocks := 0
	for id, s := range series {
		ids = append(ids, []byte(id))
		blocks := block.NewFetchBlockMetadataResults()
		metadata := block.NewFetchBlocksMetadataResult(ident.StringID(id),
			ident.NewTagsIterator(s.tags), blocks)
		for _, v := range s.data {
			numBlocks++
			entry := v
			blocks.Add(block.FetchBlockMetadataResult{
				Start:    entry.start,
				Size:     entry.size,
				Checksum: &entry.checksum,
				LastRead: entry.lastRead,
				Err:      nil,
			})
		}
		mockResult.Add(metadata)
	}

	// Setup db expectations based on test data
	opts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     includeSizes,
		IncludeChecksums: includeChecksums,
		IncludeLastRead:  includeLastRead,
	}
	mockDB.EXPECT().
		FetchBlocksMetadataV2(ctx, ident.NewIDMatcher(nsID), uint32(0), start, end,
			limit, nil, opts).
		Return(mockResult, nextPageTokenBytes, nil)

	// Run RPC method
	r, err := service.FetchBlocksMetadataRawV2(tctx, &rpc.FetchBlocksMetadataRawV2Request{
		NameSpace:        []byte(nsID),
		Shard:            0,
		RangeStart:       start.UnixNano(),
		RangeEnd:         end.UnixNano(),
		Limit:            limit,
		PageToken:        nil,
		IncludeSizes:     &includeSizes,
		IncludeChecksums: &includeChecksums,
		IncludeLastRead:  &includeLastRead,
	})
	require.NoError(t, err)

	// Assert response looks OK
	require.Equal(t, numBlocks, len(r.Elements))
	require.Equal(t, nextPageTokenBytes, r.NextPageToken)

	// Assert all blocks are present
	for _, block := range r.Elements {
		require.NotNil(t, block)

		expectedBlocks := series[string(block.ID)]

		if len(expectedBlocks.tags.Values()) == 0 {
			require.Equal(t, 0, len(block.EncodedTags))
		} else {
			encodedTags := checked.NewBytes(block.EncodedTags, nil)
			decoder := service.pools.tagDecoder.Get()
			decoder.Reset(encodedTags)

			expectedTags := ident.NewTagsIterator(expectedBlocks.tags)
			require.True(t, ident.NewTagIterMatcher(expectedTags).Matches(decoder))

			decoder.Close()
		}

		foundMatch := false
		for _, expectedBlock := range expectedBlocks.data {
			if expectedBlock.start.UnixNano() != block.Start {
				continue
			}
			foundMatch = true
			require.NotNil(t, block.Size)
			require.NotNil(t, block.Checksum)
			require.NotNil(t, block.LastRead)
		}
		require.True(t, foundMatch)
	}
}

func TestServiceFetchBlocksMetadataEndpointV2RawIsOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Setup mock db / service / context
	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	// Configure constants / options
	var (
		service          = NewService(mockDB, testTChannelThriftOptions).(*service)
		tctx, _          = tchannelthrift.NewContext(time.Minute)
		ctx              = tchannelthrift.Context(tctx)
		now              = time.Now()
		start            = now.Truncate(time.Hour)
		end              = now.Add(4 * time.Hour).Truncate(time.Hour)
		limit            = int64(2)
		includeSizes     = true
		includeChecksums = true
		includeLastRead  = true
		nsID             = "metrics"
	)

	defer ctx.Close()

	// Run RPC method
	_, err := service.FetchBlocksMetadataRawV2(tctx, &rpc.FetchBlocksMetadataRawV2Request{
		NameSpace:        []byte(nsID),
		Shard:            0,
		RangeStart:       start.UnixNano(),
		RangeEnd:         end.UnixNano(),
		Limit:            limit,
		PageToken:        nil,
		IncludeSizes:     &includeSizes,
		IncludeChecksums: &includeChecksums,
		IncludeLastRead:  &includeLastRead,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchBlocksMetadataEndpointV2RawDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	// Configure constants / options
	var (
		service          = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _          = tchannelthrift.NewContext(time.Minute)
		ctx              = tchannelthrift.Context(tctx)
		now              = time.Now()
		start            = now.Truncate(time.Hour)
		end              = now.Add(4 * time.Hour).Truncate(time.Hour)
		limit            = int64(2)
		includeSizes     = true
		includeChecksums = true
		includeLastRead  = true
		nsID             = "metrics"
	)

	defer ctx.Close()

	// Run RPC method
	_, err := service.FetchBlocksMetadataRawV2(tctx, &rpc.FetchBlocksMetadataRawV2Request{
		NameSpace:        []byte(nsID),
		Shard:            0,
		RangeStart:       start.UnixNano(),
		RangeEnd:         end.UnixNano(),
		Limit:            limit,
		PageToken:        nil,
		IncludeSizes:     &includeSizes,
		IncludeChecksums: &includeChecksums,
		IncludeLastRead:  &includeLastRead,
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceFetchTagged(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(gocontext.Background(), sp))

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	nsID := "metrics"

	streams := map[string]xio.SegmentReader{}
	series := map[string][]struct {
		t time.Time
		v float64
	}{
		"foo": {
			{start.Add(10 * time.Second), 1.0},
			{start.Add(20 * time.Second), 2.0},
		},
		"bar": {
			{start.Add(20 * time.Second), 3.0},
			{start.Add(30 * time.Second), 4.0},
		},
	}
	for id, s := range series {
		enc := testStorageOpts.EncoderPool().Get()
		enc.Reset(start, 0, nil)
		for _, v := range s {
			dp := ts.Datapoint{
				Timestamp: v.t,
				Value:     v.v,
			}
			require.NoError(t, enc.Encode(dp, xtime.Second, nil))
		}

		stream, _ := enc.Stream(ctx)
		streams[id] = stream
		mockDB.EXPECT().
			ReadEncoded(gomock.Any(), ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
			Return([][]xio.BlockReader{{
				xio.BlockReader{
					SegmentReader: stream,
				},
			}}, nil)
	}

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	md1 := doc.Metadata{
		ID: ident.BytesID("foo"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name:  []byte("baz"),
				Value: []byte("dxk"),
			},
		},
	}
	md2 := doc.Metadata{
		ID: ident.BytesID("bar"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name:  []byte("dzk"),
				Value: []byte("baz"),
			},
		},
	}

	resMap := index.NewQueryResults(ident.StringID(nsID),
		index.QueryResultsOptions{}, testIndexOptions)
	resMap.Map().Set(md1.ID, doc.NewDocumentFromMetadata(md1))
	resMap.Map().Set(md2.ID, doc.NewDocumentFromMetadata(md2))
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	mockDB.EXPECT().QueryIDs(
		gomock.Any(),
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			SeriesLimit:    int(seriesLimit),
			DocsLimit:      int(docsLimit),
		}).Return(index.QueryResult{Results: resMap, Exhaustive: true}, nil)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)

	data, err := idx.Marshal(req)
	require.NoError(t, err)
	r, err := service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:   []byte(nsID),
		Query:       data,
		RangeStart:  startNanos,
		RangeEnd:    endNanos,
		FetchData:   true,
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Elements, func(i, j int) bool {
		return bytes.Compare(r.Elements[i].ID, r.Elements[j].ID) < 0
	})
	ids := [][]byte{[]byte("bar"), []byte("foo")}
	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)

		assert.Nil(t, elem.Err)
		require.Equal(t, 1, len(elem.Segments))

		seg := elem.Segments[0]
		require.NotNil(t, seg)
		require.NotNil(t, seg.Merged)

		var expectHead, expectTail []byte
		expectSegment, err := streams[string(id)].Segment()
		require.NoError(t, err)

		if expectSegment.Head != nil {
			expectHead = expectSegment.Head.Bytes()
		}
		if expectSegment.Tail != nil {
			expectTail = expectSegment.Tail.Bytes()
		}

		assert.Equal(t, expectHead, seg.Merged.Head)
		assert.Equal(t, expectTail, seg.Merged.Tail)
	}

	sp.Finish()
	spans := mtr.FinishedSpans()

	require.Len(t, spans, 4)
	assert.Equal(t, tracepoint.FetchReadEncoded, spans[0].OperationName)
	assert.Equal(t, tracepoint.FetchReadResults, spans[1].OperationName)
	assert.Equal(t, tracepoint.FetchTagged, spans[2].OperationName)
	assert.Equal(t, "root", spans[3].OperationName)
}

func TestServiceFetchTaggedIsOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(true)

	var (
		service = NewService(mockDB, testTChannelThriftOptions).(*service)

		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)

		start = time.Now().Add(-2 * time.Hour)
		end   = start.Add(2 * time.Hour)

		nsID = "metrics"
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)

	md1 := doc.Metadata{
		ID: ident.BytesID("foo"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name:  []byte("baz"),
				Value: []byte("dxk"),
			},
		},
	}
	md2 := doc.Metadata{
		ID: ident.BytesID("bar"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name:  []byte("dzk"),
				Value: []byte("baz"),
			},
		},
	}

	resMap := index.NewQueryResults(ident.StringID(nsID),
		index.QueryResultsOptions{}, testIndexOptions)
	resMap.Map().Set(md1.ID, doc.NewDocumentFromMetadata(md1))
	resMap.Map().Set(md2.ID, doc.NewDocumentFromMetadata(md2))

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	data, err := idx.Marshal(req)
	require.NoError(t, err)
	_, err = service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:   []byte(nsID),
		Query:       data,
		RangeStart:  startNanos,
		RangeEnd:    endNanos,
		FetchData:   true,
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceFetchTaggedDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)

		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)

		start = time.Now().Add(-2 * time.Hour)
		end   = start.Add(2 * time.Hour)

		nsID = "metrics"
	)

	defer ctx.Close()
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	data, err := idx.Marshal(req)
	require.NoError(t, err)

	_, err = service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:   []byte(nsID),
		Query:       data,
		RangeStart:  startNanos,
		RangeEnd:    endNanos,
		FetchData:   true,
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceFetchTaggedNoData(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	nsID := "metrics"

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	md1 := doc.Metadata{
		ID:     ident.BytesID("foo"),
		Fields: []doc.Field{},
	}
	md2 := doc.Metadata{
		ID:     ident.BytesID("bar"),
		Fields: []doc.Field{},
	}

	resMap := index.NewQueryResults(ident.StringID(nsID),
		index.QueryResultsOptions{}, testIndexOptions)
	resMap.Map().Set(md1.ID, doc.NewDocumentFromMetadata(md1))
	resMap.Map().Set(md2.ID, doc.NewDocumentFromMetadata(md2))
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			SeriesLimit:    int(seriesLimit),
			DocsLimit:      int(docsLimit),
		}).Return(index.QueryResult{Results: resMap, Exhaustive: true}, nil)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)

	data, err := idx.Marshal(req)
	require.NoError(t, err)
	r, err := service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:   []byte(nsID),
		Query:       data,
		RangeStart:  startNanos,
		RangeEnd:    endNanos,
		FetchData:   false,
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Elements, func(i, j int) bool {
		return bytes.Compare(r.Elements[i].ID, r.Elements[j].ID) < 0
	})
	ids := [][]byte{[]byte("bar"), []byte("foo")}
	require.Equal(t, len(ids), len(r.Elements))
	for i, id := range ids {
		elem := r.Elements[i]
		require.NotNil(t, elem)
		require.Nil(t, elem.Err)
		require.Equal(t, id, elem.ID)
	}
}

func TestServiceFetchTaggedErrs(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	nsID := "metrics"

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	data, err := idx.Marshal(req)
	require.NoError(t, err)
	qry := index.Query{Query: req}

	mockDB.EXPECT().QueryIDs(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			SeriesLimit:    int(seriesLimit),
			DocsLimit:      int(docsLimit),
		}).Return(index.QueryResult{}, fmt.Errorf("random err"))
	_, err = service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:   []byte(nsID),
		Query:       data,
		RangeStart:  startNanos,
		RangeEnd:    endNanos,
		FetchData:   false,
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
	})
	require.Error(t, err)
}

func TestServiceFetchTaggedReturnOnFirstErr(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mtr := mocktracer.New()
	sp := mtr.StartSpan("root")
	ctx.SetGoContext(opentracing.ContextWithSpan(gocontext.Background(), sp))

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)
	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	nsID := "metrics"

	id := "foo"
	s := []struct {
		t time.Time
		v float64
	}{
		{start.Add(10 * time.Second), 1.0},
		{start.Add(20 * time.Second), 2.0},
	}
	enc := testStorageOpts.EncoderPool().Get()
	enc.Reset(start, 0, nil)
	for _, v := range s {
		dp := ts.Datapoint{
			Timestamp: v.t,
			Value:     v.v,
		}
		require.NoError(t, enc.Encode(dp, xtime.Second, nil))
	}

	stream, _ := enc.Stream(ctx)
	mockDB.EXPECT().
		ReadEncoded(gomock.Any(), ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), start, end).
		Return([][]xio.BlockReader{{
			xio.BlockReader{
				SegmentReader: stream,
			},
		}}, fmt.Errorf("random err")) // Return error that should trigger failure of the entire call

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	md1 := doc.Metadata{
		ID: ident.BytesID("foo"),
		Fields: []doc.Field{
			{
				Name:  []byte("foo"),
				Value: []byte("bar"),
			},
			{
				Name:  []byte("baz"),
				Value: []byte("dxk"),
			},
		},
	}

	resMap := index.NewQueryResults(ident.StringID(nsID),
		index.QueryResultsOptions{}, testIndexOptions)
	resMap.Map().Set(md1.ID, doc.NewDocumentFromMetadata(md1))
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	mockDB.EXPECT().QueryIDs(
		gomock.Any(),
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.QueryOptions{
			StartInclusive: start,
			EndExclusive:   end,
			SeriesLimit:    int(seriesLimit),
			DocsLimit:      int(docsLimit),
		}).Return(index.QueryResult{Results: resMap, Exhaustive: true}, nil)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)

	data, err := idx.Marshal(req)
	require.NoError(t, err)
	_, err = service.FetchTagged(tctx, &rpc.FetchTaggedRequest{
		NameSpace:   []byte(nsID),
		Query:       data,
		RangeStart:  startNanos,
		RangeEnd:    endNanos,
		FetchData:   true,
		SeriesLimit: &seriesLimit,
		DocsLimit:   &docsLimit,
	})
	require.Error(t, err)
}

func TestServiceAggregate(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	nsID := "metrics"

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	resMap := index.NewAggregateResults(ident.StringID(nsID),
		index.AggregateResultsOptions{}, testIndexOptions)
	resMap.Map().Set(ident.StringID("foo"), index.MustNewAggregateValues(testIndexOptions))
	resMap.Map().Set(ident.StringID("bar"), index.MustNewAggregateValues(testIndexOptions,
		ident.StringID("baz"), ident.StringID("barf")))

	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	mockDB.EXPECT().AggregateQuery(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.AggregationOptions{
			QueryOptions: index.QueryOptions{
				StartInclusive: start,
				EndExclusive:   end,
				SeriesLimit:    int(seriesLimit),
				DocsLimit:      int(docsLimit),
			},
			FieldFilter: index.AggregateFieldFilter{
				[]byte("foo"), []byte("bar"),
			},
			Type: index.AggregateTagNamesAndValues,
		}).Return(
		index.AggregateQueryResult{Results: resMap, Exhaustive: true}, nil)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)

	data, err := idx.Marshal(req)
	require.NoError(t, err)
	r, err := service.AggregateRaw(tctx, &rpc.AggregateQueryRawRequest{
		NameSpace:          []byte(nsID),
		Query:              data,
		RangeStart:         startNanos,
		RangeEnd:           endNanos,
		SeriesLimit:        &seriesLimit,
		DocsLimit:          &docsLimit,
		AggregateQueryType: rpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME_VALUE,
		TagNameFilter: [][]byte{
			[]byte("foo"), []byte("bar"),
		},
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Results, func(i, j int) bool {
		return bytes.Compare(r.Results[i].TagName, r.Results[j].TagName) < 0
	})
	require.Equal(t, 2, len(r.Results))
	require.Equal(t, "bar", string(r.Results[0].TagName))
	require.Equal(t, 2, len(r.Results[0].TagValues))
	sort.Slice(r.Results[0].TagValues, func(i, j int) bool {
		return bytes.Compare(
			r.Results[0].TagValues[i].TagValue, r.Results[0].TagValues[j].TagValue) < 0
	})
	require.Equal(t, "barf", string(r.Results[0].TagValues[0].TagValue))
	require.Equal(t, "baz", string(r.Results[0].TagValues[1].TagValue))

	require.Equal(t, "foo", string(r.Results[1].TagName))
	require.Equal(t, 0, len(r.Results[1].TagValues))
}

func TestServiceAggregateNameOnly(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Add(-2 * time.Hour)
	end := start.Add(2 * time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)
	nsID := "metrics"

	req, err := idx.NewRegexpQuery([]byte("foo"), []byte("b.*"))
	require.NoError(t, err)
	qry := index.Query{Query: req}

	resMap := index.NewAggregateResults(ident.StringID(nsID),
		index.AggregateResultsOptions{}, testIndexOptions)
	resMap.Map().Set(ident.StringID("foo"), index.AggregateValues{})
	resMap.Map().Set(ident.StringID("bar"), index.AggregateValues{})
	var (
		seriesLimit int64 = 10
		docsLimit   int64 = 10
	)
	mockDB.EXPECT().AggregateQuery(
		ctx,
		ident.NewIDMatcher(nsID),
		index.NewQueryMatcher(qry),
		index.AggregationOptions{
			QueryOptions: index.QueryOptions{
				StartInclusive: start,
				EndExclusive:   end,
				SeriesLimit:    int(seriesLimit),
				DocsLimit:      int(docsLimit),
			},
			FieldFilter: index.AggregateFieldFilter{
				[]byte("foo"), []byte("bar"),
			},
			Type: index.AggregateTagNames,
		}).Return(
		index.AggregateQueryResult{Results: resMap, Exhaustive: true}, nil)

	startNanos, err := convert.ToValue(start, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)
	endNanos, err := convert.ToValue(end, rpc.TimeType_UNIX_NANOSECONDS)
	require.NoError(t, err)

	data, err := idx.Marshal(req)
	require.NoError(t, err)
	r, err := service.AggregateRaw(tctx, &rpc.AggregateQueryRawRequest{
		NameSpace:          []byte(nsID),
		Query:              data,
		RangeStart:         startNanos,
		RangeEnd:           endNanos,
		SeriesLimit:        &seriesLimit,
		DocsLimit:          &docsLimit,
		AggregateQueryType: rpc.AggregateQueryType_AGGREGATE_BY_TAG_NAME,
		TagNameFilter: [][]byte{
			[]byte("foo"), []byte("bar"),
		},
	})
	require.NoError(t, err)

	// sort to order results to make test deterministic.
	sort.Slice(r.Results, func(i, j int) bool {
		return bytes.Compare(r.Results[i].TagName, r.Results[j].TagName) < 0
	})
	require.Equal(t, 2, len(r.Results))
	require.Equal(t, "bar", string(r.Results[0].TagName))
	require.Equal(t, 0, len(r.Results[0].TagValues))
	require.Equal(t, "foo", string(r.Results[1].TagName))
	require.Equal(t, 0, len(r.Results[1].TagValues))
}

func TestServiceWrite(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	id := "foo"

	at := time.Now().Truncate(time.Second)
	value := 42.42

	mockDB.EXPECT().
		Write(ctx, ident.NewIDMatcher(nsID), ident.NewIDMatcher(id), at, value,
			xtime.Second, nil).
		Return(nil)

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.Write(tctx, &rpc.WriteRequest{
		NameSpace: nsID,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp:         at.Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             value,
		},
	})
	require.NoError(t, err)
}

func TestServiceWriteOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mockDB.EXPECT().IsOverloaded().Return(true)
	err := service.Write(tctx, &rpc.WriteRequest{
		NameSpace: "metrics",
		ID:        "foo",
		Datapoint: &rpc.Datapoint{
			Timestamp:         time.Now().Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             42.42,
		},
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceWriteDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
	)

	defer ctx.Close()

	err := service.Write(tctx, &rpc.WriteRequest{
		NameSpace: "metrics",
		ID:        "foo",
		Datapoint: &rpc.Datapoint{
			Timestamp:         time.Now().Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             42.42,
		},
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceWriteTagged(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	var (
		nsID      = "metrics"
		id        = "foo"
		tagNames  = []string{"foo", "bar", "baz"}
		tagValues = []string{"cmon", "keep", "going"}
		at        = time.Now().Truncate(time.Second)
		value     = 42.42
	)

	mockDB.EXPECT().WriteTagged(ctx,
		ident.NewIDMatcher(nsID),
		ident.NewIDMatcher(id),
		gomock.Any(),
		at, value, xtime.Second, nil,
	).Return(nil)

	request := &rpc.WriteTaggedRequest{
		NameSpace: nsID,
		ID:        id,
		Datapoint: &rpc.Datapoint{
			Timestamp:         at.Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             value,
		},
		Tags: []*rpc.Tag{},
	}

	for i := range tagNames {
		request.Tags = append(request.Tags, &rpc.Tag{
			Name:  tagNames[i],
			Value: tagValues[i],
		})
	}
	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteTagged(tctx, request)
	require.NoError(t, err)
}

func TestServiceWriteTaggedOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mockDB.EXPECT().IsOverloaded().Return(true)
	err := service.WriteTagged(tctx, &rpc.WriteTaggedRequest{
		NameSpace: "metrics",
		ID:        "foo",
		Datapoint: &rpc.Datapoint{
			Timestamp:         time.Now().Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             42.42,
		},
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceWriteTaggedDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
	)
	defer ctx.Close()

	err := service.WriteTagged(tctx, &rpc.WriteTaggedRequest{
		NameSpace: "metrics",
		ID:        "foo",
		Datapoint: &rpc.Datapoint{
			Timestamp:         time.Now().Unix(),
			TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
			Value:             42.42,
		},
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceWriteBatchRaw(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	values := []struct {
		id string
		t  time.Time
		v  float64
	}{
		{"foo", time.Now().Truncate(time.Second), 12.34},
		{"bar", time.Now().Truncate(time.Second), 42.42},
	}

	writeBatch := writes.NewWriteBatch(len(values), ident.StringID(nsID), nil)
	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID), len(values)).
		Return(writeBatch, nil)

	mockDB.EXPECT().
		WriteBatch(ctx, ident.NewIDMatcher(nsID), writeBatch, gomock.Any()).
		Return(nil)

	var elements []*rpc.WriteBatchRawRequestElement
	for _, w := range values {
		elem := &rpc.WriteBatchRawRequestElement{
			ID: []byte(w.id),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteBatchRaw(tctx, &rpc.WriteBatchRawRequest{
		NameSpace: []byte(nsID),
		Elements:  elements,
	})
	require.NoError(t, err)
}

func TestServiceWriteBatchRawV2SingleNS(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	values := []struct {
		id string
		t  time.Time
		v  float64
	}{
		{"foo", time.Now().Truncate(time.Second), 12.34},
		{"bar", time.Now().Truncate(time.Second), 42.42},
	}

	writeBatch := writes.NewWriteBatch(len(values), ident.StringID(nsID), nil)
	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID), len(values)).
		Return(writeBatch, nil)

	mockDB.EXPECT().
		WriteBatch(ctx, ident.NewIDMatcher(nsID), writeBatch, gomock.Any()).
		Return(nil)

	var elements []*rpc.WriteBatchRawV2RequestElement
	for _, w := range values {
		elem := &rpc.WriteBatchRawV2RequestElement{
			NameSpace: 0,
			ID:        []byte(w.id),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteBatchRawV2(tctx, &rpc.WriteBatchRawV2Request{
		NameSpaces: [][]byte{[]byte(nsID)},
		Elements:   elements,
	})
	require.NoError(t, err)
}

func TestServiceWriteBatchRawV2MultiNS(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	var (
		nsID1 = "metrics"
		nsID2 = "more-metrics"

		values = []struct {
			id string
			t  time.Time
			v  float64
		}{
			{"foo", time.Now().Truncate(time.Second), 12.34},
			{"bar", time.Now().Truncate(time.Second), 42.42},
		}

		writeBatch1 = writes.NewWriteBatch(len(values), ident.StringID(nsID1), nil)
		writeBatch2 = writes.NewWriteBatch(len(values), ident.StringID(nsID2), nil)
	)

	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID1), len(values)*2).
		Return(writeBatch1, nil)
	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID2), len(values)*2).
		Return(writeBatch2, nil)

	mockDB.EXPECT().
		WriteBatch(ctx, ident.NewIDMatcher(nsID1), writeBatch1, gomock.Any()).
		Return(nil)
	mockDB.EXPECT().
		WriteBatch(ctx, ident.NewIDMatcher(nsID2), writeBatch2, gomock.Any()).
		Return(nil)

	var elements []*rpc.WriteBatchRawV2RequestElement
	for nsIdx := range []string{nsID1, nsID2} {
		for _, w := range values {
			elem := &rpc.WriteBatchRawV2RequestElement{
				NameSpace: int64(nsIdx),
				ID:        []byte(w.id),
				Datapoint: &rpc.Datapoint{
					Timestamp:         w.t.Unix(),
					TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
					Value:             w.v,
				},
			}
			elements = append(elements, elem)
		}
	}

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteBatchRawV2(tctx, &rpc.WriteBatchRawV2Request{
		NameSpaces: [][]byte{[]byte(nsID1), []byte(nsID2)},
		Elements:   elements,
	})
	require.NoError(t, err)
}

func TestServiceWriteBatchRawOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mockDB.EXPECT().IsOverloaded().Return(true)
	err := service.WriteBatchRaw(tctx, &rpc.WriteBatchRawRequest{
		NameSpace: []byte("metrics"),
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

// TestServiceWriteBatchRawOverMaxOutstandingRequests tests that the WriteBatchRaw endpoint
// will reject requests if the number of outstanding write requests has hit the maximum.
func TestServiceWriteBatchRawOverMaxOutstandingRequests(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	tchanOpts := testTChannelThriftOptions.
		SetMaxOutstandingWriteRequests(1)
	service := NewService(mockDB, tchanOpts).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	values := []struct {
		id string
		t  time.Time
		v  float64
	}{
		{"foo", time.Now().Truncate(time.Second), 12.34},
		{"bar", time.Now().Truncate(time.Second), 42.42},
	}

	var (
		testIsComplete       = make(chan struct{}, 0)
		requestIsOutstanding = make(chan struct{}, 0)
	)
	writeBatch := writes.NewWriteBatch(len(values), ident.StringID(nsID), nil)
	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID), len(values)).
		Do(func(nsID ident.ID, numValues int) {
			// Signal that a request is now outstanding.
			close(requestIsOutstanding)
			// Wait for test to complete.
			<-testIsComplete
		}).
		Return(writeBatch, nil)
	mockDB.EXPECT().
		WriteBatch(ctx, ident.NewIDMatcher(nsID), writeBatch, gomock.Any()).
		Return(nil).
		// AnyTimes() so we don't have to add extra signaling to wait for the
		// async goroutine to complete.
		AnyTimes()

	var elements []*rpc.WriteBatchRawRequestElement
	for _, w := range values {
		elem := &rpc.WriteBatchRawRequestElement{
			ID: []byte(w.id),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	mockDB.EXPECT().IsOverloaded().Return(false).AnyTimes()

	// First request will hang until the test is over (so a request is outstanding).
	outstandingRequestIsComplete := make(chan struct{}, 0)
	go func() {
		service.WriteBatchRaw(tctx, &rpc.WriteBatchRawRequest{
			NameSpace: []byte(nsID),
			Elements:  elements,
		})
		close(outstandingRequestIsComplete)
	}()
	<-requestIsOutstanding

	// Second request should get an overloaded error since there is an outstanding request.
	err := service.WriteBatchRaw(tctx, &rpc.WriteBatchRawRequest{
		NameSpace: []byte(nsID),
		Elements:  elements,
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
	close(testIsComplete)

	// Ensure the number of outstanding requests gets decremented at the end of the R.P.C.
	<-outstandingRequestIsComplete
	require.Equal(t, 0, service.state.numOutstandingWriteRPCs)
}

func TestServiceWriteBatchRawDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
	)
	defer ctx.Close()

	err := service.WriteBatchRaw(tctx, &rpc.WriteBatchRawRequest{
		NameSpace: []byte("metrics"),
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceWriteTaggedBatchRaw(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	mockDecoder := serialize.NewMockTagDecoder(ctrl)
	mockDecoder.EXPECT().Reset(gomock.Any()).AnyTimes()
	mockDecoder.EXPECT().Err().Return(nil).AnyTimes()
	mockDecoder.EXPECT().Close().AnyTimes()
	mockDecoderPool := serialize.NewMockTagDecoderPool(ctrl)
	mockDecoderPool.EXPECT().Get().Return(mockDecoder).AnyTimes()

	opts := tchannelthrift.NewOptions().
		SetTagDecoderPool(mockDecoderPool)

	service := NewService(mockDB, opts).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	values := []struct {
		id        string
		tagEncode string
		t         time.Time
		v         float64
	}{
		{"foo", "a|b", time.Now().Truncate(time.Second), 12.34},
		{"bar", "c|dd", time.Now().Truncate(time.Second), 42.42},
	}

	writeBatch := writes.NewWriteBatch(len(values), ident.StringID(nsID), nil)
	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID), len(values)).
		Return(writeBatch, nil)

	mockDB.EXPECT().
		WriteTaggedBatch(ctx, ident.NewIDMatcher(nsID), writeBatch, gomock.Any()).
		Return(nil)

	var elements []*rpc.WriteTaggedBatchRawRequestElement
	for _, w := range values {
		elem := &rpc.WriteTaggedBatchRawRequestElement{
			ID:          []byte(w.id),
			EncodedTags: []byte(w.tagEncode),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteTaggedBatchRaw(tctx, &rpc.WriteTaggedBatchRawRequest{
		NameSpace: []byte(nsID),
		Elements:  elements,
	})
	require.NoError(t, err)
}

func TestServiceWriteTaggedBatchRawV2(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	mockDecoder := serialize.NewMockTagDecoder(ctrl)
	mockDecoder.EXPECT().Reset(gomock.Any()).AnyTimes()
	mockDecoder.EXPECT().Err().Return(nil).AnyTimes()
	mockDecoder.EXPECT().Close().AnyTimes()
	mockDecoderPool := serialize.NewMockTagDecoderPool(ctrl)
	mockDecoderPool.EXPECT().Get().Return(mockDecoder).AnyTimes()

	opts := tchannelthrift.NewOptions().
		SetTagDecoderPool(mockDecoderPool)

	service := NewService(mockDB, opts).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	values := []struct {
		id        string
		tagEncode string
		t         time.Time
		v         float64
	}{
		{"foo", "a|b", time.Now().Truncate(time.Second), 12.34},
		{"bar", "c|dd", time.Now().Truncate(time.Second), 42.42},
	}

	writeBatch := writes.NewWriteBatch(len(values), ident.StringID(nsID), nil)
	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID), len(values)).
		Return(writeBatch, nil)

	mockDB.EXPECT().
		WriteTaggedBatch(ctx, ident.NewIDMatcher(nsID), writeBatch, gomock.Any()).
		Return(nil)

	var elements []*rpc.WriteTaggedBatchRawV2RequestElement
	for _, w := range values {
		elem := &rpc.WriteTaggedBatchRawV2RequestElement{
			NameSpace:   0,
			ID:          []byte(w.id),
			EncodedTags: []byte(w.tagEncode),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteTaggedBatchRawV2(tctx, &rpc.WriteTaggedBatchRawV2Request{
		NameSpaces: [][]byte{[]byte(nsID)},
		Elements:   elements,
	})
	require.NoError(t, err)
}

func TestServiceWriteTaggedBatchRawV2MultiNS(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	mockDecoder := serialize.NewMockTagDecoder(ctrl)
	mockDecoder.EXPECT().Reset(gomock.Any()).AnyTimes()
	mockDecoder.EXPECT().Err().Return(nil).AnyTimes()
	mockDecoder.EXPECT().Close().AnyTimes()
	mockDecoderPool := serialize.NewMockTagDecoderPool(ctrl)
	mockDecoderPool.EXPECT().Get().Return(mockDecoder).AnyTimes()

	opts := tchannelthrift.NewOptions().
		SetTagDecoderPool(mockDecoderPool)

	service := NewService(mockDB, opts).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	var (
		nsID1  = "metrics"
		nsID2  = "more-metrics"
		values = []struct {
			id        string
			tagEncode string
			t         time.Time
			v         float64
		}{
			{"foo", "a|b", time.Now().Truncate(time.Second), 12.34},
			{"bar", "c|dd", time.Now().Truncate(time.Second), 42.42},
		}
		writeBatch1 = writes.NewWriteBatch(len(values), ident.StringID(nsID1), nil)
		writeBatch2 = writes.NewWriteBatch(len(values), ident.StringID(nsID2), nil)
	)

	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID1), len(values)*2).
		Return(writeBatch1, nil)
	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID2), len(values)*2).
		Return(writeBatch2, nil)

	mockDB.EXPECT().
		WriteTaggedBatch(ctx, ident.NewIDMatcher(nsID1), writeBatch1, gomock.Any()).
		Return(nil)
	mockDB.EXPECT().
		WriteTaggedBatch(ctx, ident.NewIDMatcher(nsID2), writeBatch2, gomock.Any()).
		Return(nil)

	var elements []*rpc.WriteTaggedBatchRawV2RequestElement
	for nsIdx := range []string{nsID1, nsID2} {
		for _, w := range values {
			elem := &rpc.WriteTaggedBatchRawV2RequestElement{
				NameSpace:   int64(nsIdx),
				ID:          []byte(w.id),
				EncodedTags: []byte(w.tagEncode),
				Datapoint: &rpc.Datapoint{
					Timestamp:         w.t.Unix(),
					TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
					Value:             w.v,
				},
			}
			elements = append(elements, elem)
		}
	}

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteTaggedBatchRawV2(tctx, &rpc.WriteTaggedBatchRawV2Request{
		NameSpaces: [][]byte{[]byte(nsID1), []byte(nsID2)},
		Elements:   elements,
	})
	require.NoError(t, err)
}

func TestServiceWriteTaggedBatchRawOverloaded(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mockDB.EXPECT().IsOverloaded().Return(true)
	err := service.WriteTaggedBatchRaw(tctx, &rpc.WriteTaggedBatchRawRequest{
		NameSpace: []byte("metrics"),
	})
	require.Equal(t, tterrors.NewInternalError(errServerIsOverloaded), err)
}

func TestServiceWriteTaggedBatchRawDatabaseNotSet(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		service = NewService(nil, testTChannelThriftOptions).(*service)
		tctx, _ = tchannelthrift.NewContext(time.Minute)
		ctx     = tchannelthrift.Context(tctx)
	)
	defer ctx.Close()

	err := service.WriteTaggedBatchRaw(tctx, &rpc.WriteTaggedBatchRawRequest{
		NameSpace: []byte("metrics"),
	})
	require.Equal(t, tterrors.NewInternalError(errDatabaseIsNotInitializedYet), err)
}

func TestServiceWriteTaggedBatchRawUnknownError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()

	mockDecoder := serialize.NewMockTagDecoder(ctrl)
	mockDecoder.EXPECT().Reset(gomock.Any()).AnyTimes()
	mockDecoder.EXPECT().Err().Return(nil).AnyTimes()
	mockDecoder.EXPECT().Close().AnyTimes()
	mockDecoderPool := serialize.NewMockTagDecoderPool(ctrl)
	mockDecoderPool.EXPECT().Get().Return(mockDecoder).AnyTimes()

	opts := tchannelthrift.NewOptions().
		SetTagDecoderPool(mockDecoderPool)

	service := NewService(mockDB, opts).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"
	unknownErr := fmt.Errorf("unknown-err")
	values := []struct {
		id        string
		tagEncode string
		t         time.Time
		v         float64
	}{
		{"foo", "a|b", time.Now().Truncate(time.Second), 12.34},
		{"bar", "c|dd", time.Now().Truncate(time.Second), 42.42},
	}

	mockDB.EXPECT().
		BatchWriter(ident.NewIDMatcher(nsID), len(values)).
		Return(nil, unknownErr)

	var elements []*rpc.WriteTaggedBatchRawRequestElement
	for _, w := range values {
		elem := &rpc.WriteTaggedBatchRawRequestElement{
			ID:          []byte(w.id),
			EncodedTags: []byte(w.tagEncode),
			Datapoint: &rpc.Datapoint{
				Timestamp:         w.t.Unix(),
				TimestampTimeType: rpc.TimeType_UNIX_SECONDS,
				Value:             w.v,
			},
		}
		elements = append(elements, elem)
	}

	mockDB.EXPECT().IsOverloaded().Return(false)
	err := service.WriteTaggedBatchRaw(tctx, &rpc.WriteTaggedBatchRawRequest{
		NameSpace: []byte(nsID),
		Elements:  elements,
	})
	require.Error(t, err)
	require.Equal(t, convert.ToRPCError(unknownErr), err)
}

func TestServiceRepair(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	mockDB.EXPECT().Repair().Return(nil)

	err := service.Repair(tctx)
	require.NoError(t, err)
}

func TestServiceTruncate(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	nsID := "metrics"

	truncated := int64(123)

	mockDB.EXPECT().Truncate(ident.NewIDMatcher(nsID)).Return(truncated, nil)

	r, err := service.Truncate(tctx, &rpc.TruncateRequest{NameSpace: []byte(nsID)})
	require.NoError(t, err)
	assert.Equal(t, truncated, r.NumSeries)
}

func TestServiceSetPersistRateLimit(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions()
	runtimeOpts = runtimeOpts.SetPersistRateLimitOptions(
		runtimeOpts.PersistRateLimitOptions().
			SetLimitEnabled(false))
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetPersistRateLimit(tctx)
	require.NoError(t, err)
	assert.Equal(t, false, getResp.LimitEnabled)

	enable := true
	req := &rpc.NodeSetPersistRateLimitRequest{
		LimitEnabled: &enable,
	}
	setResp, err := service.SetPersistRateLimit(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, true, setResp.LimitEnabled)
}

func TestServiceSetWriteNewSeriesAsync(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions().
		SetWriteNewSeriesAsync(false)
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetWriteNewSeriesAsync(tctx)
	require.NoError(t, err)
	assert.Equal(t, false, getResp.WriteNewSeriesAsync)

	req := &rpc.NodeSetWriteNewSeriesAsyncRequest{
		WriteNewSeriesAsync: true,
	}
	setResp, err := service.SetWriteNewSeriesAsync(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, true, setResp.WriteNewSeriesAsync)
}

func TestServiceSetWriteNewSeriesBackoffDuration(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions().
		SetWriteNewSeriesBackoffDuration(3 * time.Millisecond)
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetWriteNewSeriesBackoffDuration(tctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), getResp.WriteNewSeriesBackoffDuration)
	assert.Equal(t, rpc.TimeType_UNIX_MILLISECONDS, getResp.DurationType)

	req := &rpc.NodeSetWriteNewSeriesBackoffDurationRequest{
		WriteNewSeriesBackoffDuration: 1,
		DurationType:                  rpc.TimeType_UNIX_SECONDS,
	}
	setResp, err := service.SetWriteNewSeriesBackoffDuration(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), setResp.WriteNewSeriesBackoffDuration)
	assert.Equal(t, rpc.TimeType_UNIX_MILLISECONDS, setResp.DurationType)
}

func TestServiceSetWriteNewSeriesLimitPerShardPerSecond(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	runtimeOpts := runtime.NewOptions().
		SetWriteNewSeriesLimitPerShardPerSecond(42)
	runtimeOptsMgr := runtime.NewOptionsManager()
	require.NoError(t, runtimeOptsMgr.Update(runtimeOpts))
	opts := testStorageOpts.SetRuntimeOptionsManager(runtimeOptsMgr)

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(opts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false).AnyTimes()

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	getResp, err := service.GetWriteNewSeriesLimitPerShardPerSecond(tctx)
	require.NoError(t, err)
	assert.Equal(t, int64(42), getResp.WriteNewSeriesLimitPerShardPerSecond)

	req := &rpc.NodeSetWriteNewSeriesLimitPerShardPerSecondRequest{
		WriteNewSeriesLimitPerShardPerSecond: 84,
	}
	setResp, err := service.SetWriteNewSeriesLimitPerShardPerSecond(tctx, req)
	require.NoError(t, err)
	assert.Equal(t, int64(84), setResp.WriteNewSeriesLimitPerShardPerSecond)
}

func TestServiceAggregateTiles(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mockDB := storage.NewMockDatabase(ctrl)
	mockDB.EXPECT().Options().Return(testStorageOpts).AnyTimes()
	mockDB.EXPECT().IsOverloaded().Return(false)

	service := NewService(mockDB, testTChannelThriftOptions).(*service)

	tctx, _ := tchannelthrift.NewContext(time.Minute)
	ctx := tchannelthrift.Context(tctx)
	defer ctx.Close()

	start := time.Now().Truncate(time.Hour).Add(-1 * time.Hour)
	end := start.Add(time.Hour)

	start, end = start.Truncate(time.Second), end.Truncate(time.Second)

	step := "10m"
	stepDuration, err := time.ParseDuration(step)
	require.NoError(t, err)

	sourceNsID := "source"
	targetNsID := "target"

	mockDB.EXPECT().AggregateTiles(
		ctx,
		ident.NewIDMatcher(sourceNsID),
		ident.NewIDMatcher(targetNsID),
		gomock.Any(),
	).DoAndReturn(func(gotCtx, gotSourceNsID, gotTargetNsID interface{}, opts storage.AggregateTilesOptions) (int64, error) {
		require.NotNil(t, opts)
		require.Equal(t, start, opts.Start)
		require.Equal(t, end, opts.End)
		require.Equal(t, stepDuration, opts.Step)
		require.NotNil(t, opts.InsOptions)
		return int64(4), nil
	})

	result, err := service.AggregateTiles(tctx, &rpc.AggregateTilesRequest{
		SourceNamespace: sourceNsID,
		TargetNamespace: targetNsID,
		RangeStart:      start.Unix(),
		RangeEnd:        end.Unix(),
		Step:            step,
		RangeType:       rpc.TimeType_UNIX_SECONDS,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4), result.ProcessedTileCount)
}
