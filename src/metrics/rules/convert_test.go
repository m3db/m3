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

package rules

import (
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestToAggregationIDAndStoragePoliciesNilPolicyProto(t *testing.T) {
	policiesProto := []*policypb.Policy{nil}
	_, _, err := toAggregationIDAndStoragePolicies(policiesProto)
	require.Equal(t, errNilPolicyProto, err)
}

func TestToAggregationIDAndStoragePoliciesNilStoragePolicyProto(t *testing.T) {
	policiesProto := []*policypb.Policy{
		&policypb.Policy{},
	}
	_, _, err := toAggregationIDAndStoragePolicies(policiesProto)
	require.Error(t, err)
}

func TestToAggregationIDAndStoragePoliciesInvalidStoragePolicyProto(t *testing.T) {
	policiesProto := []*policypb.Policy{
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{Precision: 1234},
				Retention:  policypb.Retention{Period: 5678},
			},
		},
	}
	_, _, err := toAggregationIDAndStoragePolicies(policiesProto)
	require.Error(t, err)
}

func TestToAggregationIDAndStoragePoliciesInvalidAggregationTypes(t *testing.T) {
	policiesProto := []*policypb.Policy{
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{10, 1234567},
		},
	}
	_, _, err := toAggregationIDAndStoragePolicies(policiesProto)
	require.Error(t, err)
}

func TestToAggregationIDAndStoragePoliciesInconsistentAggregationIDs(t *testing.T) {
	policiesProto := []*policypb.Policy{
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{1},
		},
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 720 * time.Hour.Nanoseconds(),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{2},
		},
	}
	_, _, err := toAggregationIDAndStoragePolicies(policiesProto)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "more than one aggregation ID in legacy policies list proto: ID1=Last, ID2=Min"))
}

func TestToAggregationIDAndStoragePoliciesDefaultAggregationID(t *testing.T) {
	policiesProto := []*policypb.Policy{
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
		},
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 720 * time.Hour.Nanoseconds(),
				},
			},
		},
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: time.Hour.Nanoseconds(),
					Precision:  time.Hour.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 365 * 24 * time.Hour.Nanoseconds(),
				},
			},
		},
	}
	expectedStoragePolicies := policy.StoragePolicies{
		policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
		policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
	}
	aggregationID, storagePolicies, err := toAggregationIDAndStoragePolicies(policiesProto)
	require.NoError(t, err)
	require.Equal(t, aggregation.DefaultID, aggregationID)
	require.Equal(t, expectedStoragePolicies, storagePolicies)
}

func TestToAggregationIDAndStoragePoliciesCustomAggregationID(t *testing.T) {
	policiesProto := []*policypb.Policy{
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{1, 2},
		},
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 720 * time.Hour.Nanoseconds(),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{1, 2},
		},
		&policypb.Policy{
			StoragePolicy: &policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: time.Hour.Nanoseconds(),
					Precision:  time.Hour.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 365 * 24 * time.Hour.Nanoseconds(),
				},
			},
			AggregationTypes: []aggregationpb.AggregationType{1, 2},
		},
	}
	expectedStoragePolicies := policy.StoragePolicies{
		policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
		policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
	}
	aggregationID, storagePolicies, err := toAggregationIDAndStoragePolicies(policiesProto)
	require.NoError(t, err)
	require.Equal(t, aggregation.MustCompressTypes(aggregation.Last, aggregation.Min), aggregationID)
	require.Equal(t, expectedStoragePolicies, storagePolicies)
}
