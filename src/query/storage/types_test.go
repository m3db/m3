// Copyright (c) 2019 Uber Technologies, Inc.
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

package storage

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/generated/proto/rpcpb"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRestrictFetchOptionsFromProto(t *testing.T) {
	tests := []struct {
		value       *rpcpb.RestrictFetchOptions
		expected    RestrictFetchOptions
		errContains string
	}{
		{
			value: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE,
			},
			expected: RestrictFetchOptions{
				MetricsType: UnaggregatedMetricsType,
			},
		},
		{
			value: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
				MetricsStoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(time.Second),
					},
					Retention: &policypb.Retention{
						Period: int64(24 * time.Hour),
					},
				},
			},
			expected: RestrictFetchOptions{
				MetricsType: AggregatedMetricsType,
				StoragePolicy: policy.NewStoragePolicy(time.Minute,
					xtime.Second, 24*time.Hour),
			},
		},
		{
			value:       nil,
			errContains: errNoRestrictFetchOptionsProtoMsg.Error(),
		},
		{
			value: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_UNKNOWN_METRICS_TYPE,
			},
			errContains: "unknown metrics type:",
		},
		{
			value: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE,
				MetricsStoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(time.Second),
					},
					Retention: &policypb.Retention{
						Period: int64(24 * time.Hour),
					},
				},
			},
			errContains: "expected no storage policy for unaggregated metrics",
		},
		{
			value: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
				MetricsStoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: -1,
					},
				},
			},
			errContains: "unable to convert from duration to time unit",
		},
		{
			value: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
				MetricsStoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(-1),
					},
				},
			},
			errContains: "unable to convert from duration to time unit",
		},
		{
			value: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
				MetricsStoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(time.Second),
					},
					Retention: &policypb.Retention{
						Period: int64(-1),
					},
				},
			},
			errContains: "expected positive retention",
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.value), func(t *testing.T) {
			result, err := NewRestrictFetchOptionsFromProto(test.value)
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

func TestRestrictFetchOptionsProto(t *testing.T) {
	tests := []struct {
		value       RestrictFetchOptions
		expected    *rpcpb.RestrictFetchOptions
		errContains string
	}{
		{
			value: RestrictFetchOptions{
				MetricsType: UnaggregatedMetricsType,
			},
			expected: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_UNAGGREGATED_METRICS_TYPE,
			},
		},
		{
			value: RestrictFetchOptions{
				MetricsType: AggregatedMetricsType,
				StoragePolicy: policy.NewStoragePolicy(time.Minute,
					xtime.Second, 24*time.Hour),
			},
			expected: &rpcpb.RestrictFetchOptions{
				MetricsType: rpcpb.MetricsType_AGGREGATED_METRICS_TYPE,
				MetricsStoragePolicy: &policypb.StoragePolicy{
					Resolution: &policypb.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(time.Second),
					},
					Retention: &policypb.Retention{
						Period: int64(24 * time.Hour),
					},
				},
			},
		},
		{
			value: RestrictFetchOptions{
				MetricsType: MetricsType(uint(math.MaxUint16)),
			},
			errContains: "unknown metrics type:",
		},
		{
			value: RestrictFetchOptions{
				MetricsType: UnaggregatedMetricsType,
				StoragePolicy: policy.NewStoragePolicy(time.Minute,
					xtime.Second, 24*time.Hour),
			},
			errContains: "expected no storage policy for unaggregated metrics",
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%s", test.value), func(t *testing.T) {
			result, err := test.value.Proto()
			if test.errContains == "" {
				require.NoError(t, err)
				assert.Equal(t, test.expected, result)
				return
			}

			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), test.errContains))
		})
	}
}
