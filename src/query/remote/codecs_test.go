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
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/rpcpb"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"

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
		{
			Datapoints: valList0,
			Tags:       encodeTags(tags0),
		},
		{
			Datapoints: valList1,
			Tags:       encodeTags(tags1),
		},
		{
			Datapoints: valList2,
			Tags:       encodeTags(tags1),
		},
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
	fetchOpts := storage.NewFetchOptions()
	fetchOpts.SeriesLimit = 42
	fetchOpts.RestrictQueryOptions = &storage.RestrictQueryOptions{
		RestrictByType: &storage.RestrictByType{
			MetricsType:   storagemetadata.AggregatedMetricsType,
			StoragePolicy: policy.MustParseStoragePolicy("1m:14d"),
		},
	}
	lookback := time.Minute
	fetchOpts.LookbackDuration = &lookback

	grpcQ, err := encodeFetchRequest(rQ, fetchOpts)
	require.NoError(t, err)
	require.NotNil(t, grpcQ)
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
	require.NotNil(t, grpcQ.Options)
	assert.Equal(t, int64(42), grpcQ.Options.Limit)
	require.NotNil(t, grpcQ.Options.Restrict)
	require.NotNil(t, grpcQ.Options.Restrict.RestrictQueryType)
	assert.Equal(t, rpc.MetricsType_AGGREGATED_METRICS_TYPE,
		grpcQ.Options.Restrict.RestrictQueryType.MetricsType)
	require.NotNil(t, grpcQ.Options.Restrict.RestrictQueryType.MetricsStoragePolicy)
	expectedStoragePolicyProto, err := fetchOpts.RestrictQueryOptions.
		RestrictByType.StoragePolicy.Proto()
	require.NoError(t, err)
	assert.Equal(t, expectedStoragePolicyProto, grpcQ.Options.Restrict.
		RestrictQueryType.MetricsStoragePolicy)
	assert.Equal(t, lookback, time.Duration(grpcQ.Options.LookbackDuration))
}

func TestEncodeDecodeFetchQuery(t *testing.T) {
	rQ, _, _ := createStorageFetchQuery(t)
	fetchOpts := storage.NewFetchOptions()
	fetchOpts.SeriesLimit = 42
	fetchOpts.RestrictQueryOptions = &storage.RestrictQueryOptions{
		RestrictByType: &storage.RestrictByType{
			MetricsType:   storagemetadata.AggregatedMetricsType,
			StoragePolicy: policy.MustParseStoragePolicy("1m:14d"),
		},
	}
	lookback := time.Minute
	fetchOpts.LookbackDuration = &lookback

	gq, err := encodeFetchRequest(rQ, fetchOpts)
	require.NoError(t, err)
	reverted, err := decodeFetchRequest(gq)
	require.NoError(t, err)
	readQueriesAreEqual(t, rQ, reverted)
	revertedOpts, err := decodeFetchOptions(gq.GetOptions())
	require.NoError(t, err)
	require.NotNil(t, revertedOpts)
	require.Equal(t, fetchOpts.SeriesLimit, revertedOpts.SeriesLimit)
	require.Equal(t, fetchOpts.RestrictQueryOptions.
		RestrictByType.MetricsType,
		revertedOpts.RestrictQueryOptions.RestrictByType.MetricsType)
	require.Equal(t, fetchOpts.RestrictQueryOptions.
		RestrictByType.StoragePolicy.String(),
		revertedOpts.RestrictQueryOptions.RestrictByType.StoragePolicy.String())
	require.NotNil(t, revertedOpts.LookbackDuration)
	require.Equal(t, lookback, *revertedOpts.LookbackDuration)

	// Encode again
	gqr, err := encodeFetchRequest(reverted, revertedOpts)
	require.NoError(t, err)
	assert.Equal(t, gq, gqr)
}

func TestEncodeMetadata(t *testing.T) {
	headers := make(http.Header)
	headers.Add("Foo", "bar")
	headers.Add("Foo", "baz")
	headers.Add("Foo", "abc")
	headers.Add("lorem", "ipsum")
	ctx := context.WithValue(context.Background(), handleroptions.RequestHeaderKey, headers)
	requestID := "requestID"

	encodedCtx := encodeMetadata(ctx, requestID)
	md, ok := metadata.FromOutgoingContext(encodedCtx)
	require.True(t, ok)
	assert.Equal(t, []string{"bar", "baz", "abc"}, md["foo"], "metadat keys must be lower case")
	assert.Equal(t, []string{"ipsum"}, md["lorem"])
	assert.Equal(t, []string{requestID}, md[reqIDKey])
}

func TestRetrieveMetadata(t *testing.T) {
	headers := make(http.Header)
	headers.Add("Foo", "bar")
	headers.Add("Foo", "baz")
	headers.Add("Foo", "abc")
	headers.Add("Lorem", "ipsum")
	requestID := "requestID"
	headers[reqIDKey] = []string{requestID}
	ctx := metadata.NewIncomingContext(context.TODO(), metadata.MD(headers))
	encodedCtx := retrieveMetadata(ctx, instrument.NewOptions())

	require.Equal(t, requestID, logging.ReadContextID(encodedCtx))
}

func TestNewRestrictQueryOptionsFromProto(t *testing.T) {
	tests := []struct {
		value       *rpcpb.RestrictQueryOptions
		expected    *storage.RestrictQueryOptions
		errContains string
	}{
		{
			value: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE,
				},
			},
			expected: &storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
				},
			},
		},
		{
			value: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
					MetricsStoragePolicy: &policypb.StoragePolicy{
						Resolution: policypb.Resolution{
							WindowSize: int64(time.Minute),
							Precision:  int64(time.Second),
						},
						Retention: policypb.Retention{
							Period: int64(24 * time.Hour),
						},
					},
				},
				RestrictQueryTags: &rpc.RestrictQueryTags{
					Restrict: &rpc.TagMatchers{
						TagMatchers: []*rpc.TagMatcher{
							newRPCMatcher(rpc.MatcherType_NOTREGEXP, "foo", "bar"),
							newRPCMatcher(rpc.MatcherType_EQUAL, "baz", "qux"),
						},
					},
					Strip: [][]byte{
						[]byte("foobar"),
					},
				},
			},
			expected: &storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType: storagemetadata.AggregatedMetricsType,
					StoragePolicy: policy.NewStoragePolicy(time.Minute,
						xtime.Second, 24*time.Hour),
				},
				RestrictByTag: &storage.RestrictByTag{
					Restrict: []models.Matcher{
						mustNewMatcher(models.MatchNotRegexp, "foo", "bar"),
						mustNewMatcher(models.MatchEqual, "baz", "qux"),
					},
					Strip: [][]byte{
						[]byte("foobar"),
					},
				},
			},
		},
		{
			value: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_UNKNOWN_METRICS_TYPE,
				},
			},
			errContains: "unknown metrics type:",
		},
		{
			value: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE,
					MetricsStoragePolicy: &policypb.StoragePolicy{
						Resolution: policypb.Resolution{
							WindowSize: int64(time.Minute),
							Precision:  int64(time.Second),
						},
						Retention: policypb.Retention{
							Period: int64(24 * time.Hour),
						},
					},
				},
			},
			errContains: "expected no storage policy for unaggregated metrics",
		},
		{
			value: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
					MetricsStoragePolicy: &policypb.StoragePolicy{
						Resolution: policypb.Resolution{
							WindowSize: -1,
						},
					},
				},
			},
			errContains: "unable to convert from duration to time unit",
		},
		{
			value: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
					MetricsStoragePolicy: &policypb.StoragePolicy{
						Resolution: policypb.Resolution{
							WindowSize: int64(time.Minute),
							Precision:  int64(-1),
						},
					},
				},
			},
			errContains: "unable to convert from duration to time unit",
		},
		{
			value: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
					MetricsStoragePolicy: &policypb.StoragePolicy{
						Resolution: policypb.Resolution{
							WindowSize: int64(time.Minute),
							Precision:  int64(time.Second),
						},
						Retention: policypb.Retention{
							Period: int64(-1),
						},
					},
				},
			},
			errContains: "expected positive retention",
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%+v", test), func(t *testing.T) {
			result, err := decodeRestrictQueryOptions(test.value)
			if test.errContains == "" {
				require.NoError(t, err)
				assert.Equal(t, test.expected, result)
				return
			}

			require.Error(t, err)
			assert.True(t,
				strings.Contains(err.Error(), test.errContains),
				fmt.Sprintf("err=%v, want_contains=%v", err.Error(), test.errContains))
		})
	}
}

func mustNewMatcher(t models.MatchType, n, v string) models.Matcher {
	matcher, err := models.NewMatcher(t, []byte(n), []byte(v))
	if err != nil {
		panic(err)
	}

	return matcher
}

func newRPCMatcher(t rpc.MatcherType, n, v string) *rpc.TagMatcher {
	return &rpc.TagMatcher{Name: []byte(n), Value: []byte(v), Type: t}
}

func TestRestrictQueryOptionsProto(t *testing.T) {
	tests := []struct {
		value       storage.RestrictQueryOptions
		expected    *rpcpb.RestrictQueryOptions
		errContains string
	}{
		{
			value: storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
				},
				RestrictByTag: &storage.RestrictByTag{
					Restrict: []models.Matcher{
						mustNewMatcher(models.MatchNotRegexp, "foo", "bar"),
					},
					Strip: [][]byte{[]byte("foobar")},
				},
			},
			expected: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE,
				},
				RestrictQueryTags: &rpcpb.RestrictQueryTags{
					Restrict: &rpc.TagMatchers{
						TagMatchers: []*rpc.TagMatcher{
							newRPCMatcher(rpc.MatcherType_NOTREGEXP, "foo", "bar"),
						},
					},
					Strip: [][]byte{[]byte("foobar")},
				},
			},
		},
		{
			value: storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType: storagemetadata.AggregatedMetricsType,
					StoragePolicy: policy.NewStoragePolicy(time.Minute,
						xtime.Second, 24*time.Hour),
				},
				RestrictByTag: &storage.RestrictByTag{
					Restrict: models.Matchers{
						mustNewMatcher(models.MatchNotRegexp, "foo", "bar"),
						mustNewMatcher(models.MatchEqual, "baz", "qux"),
					},
					Strip: [][]byte{[]byte("foobar")},
				},
			},
			expected: &rpcpb.RestrictQueryOptions{
				RestrictQueryType: &rpcpb.RestrictQueryType{
					MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
					MetricsStoragePolicy: &policypb.StoragePolicy{
						Resolution: policypb.Resolution{
							WindowSize: int64(time.Minute),
							Precision:  int64(time.Second),
						},
						Retention: policypb.Retention{
							Period: int64(24 * time.Hour),
						},
					},
				},
				RestrictQueryTags: &rpcpb.RestrictQueryTags{
					Restrict: &rpc.TagMatchers{
						TagMatchers: []*rpc.TagMatcher{
							newRPCMatcher(rpc.MatcherType_NOTREGEXP, "foo", "bar"),
							newRPCMatcher(rpc.MatcherType_EQUAL, "baz", "qux"),
						},
					},
					Strip: [][]byte{[]byte("foobar")},
				},
			},
		},
		{
			value: storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType: storagemetadata.MetricsType(uint(math.MaxUint16)),
				},
			},
			errContains: "unknown metrics type:",
		},
		{
			value: storage.RestrictQueryOptions{
				RestrictByType: &storage.RestrictByType{
					MetricsType: storagemetadata.UnaggregatedMetricsType,
					StoragePolicy: policy.NewStoragePolicy(time.Minute,
						xtime.Second, 24*time.Hour),
				},
			},
			errContains: "expected no storage policy for unaggregated metrics",
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%+v", test.value), func(t *testing.T) {
			result, err := encodeRestrictQueryOptions(&test.value)
			if test.errContains == "" {
				require.NoError(t, err)
				require.Equal(t, test.expected, result)
				return
			}

			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), test.errContains))
		})
	}
}
