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
	"errors"
	"fmt"
	"sort"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/policy"
)

var (
	errNilPolicyProto = errors.New("nil policy proto")
)

// toAggregationIDAndStoragePolicies converts a list of policies to a single
// aggregation ID and a list of storage policies. This is needed for the purpose
// of transparently evolving v1 mapping rule protos and rollup target protos into
// v2 protos and associated data structures.
func toAggregationIDAndStoragePolicies(
	pbPolicies []*policypb.Policy,
) (aggregation.ID, policy.StoragePolicies, error) {
	var (
		aggregationID      aggregation.ID
		firstTime          = true
		storagePoliciesSet = make(map[policy.StoragePolicy]struct{}, len(pbPolicies))
	)
	for _, pbPolicy := range pbPolicies {
		if pbPolicy == nil {
			return aggregation.DefaultID, nil, errNilPolicyProto
		}
		sp, err := policy.NewStoragePolicyFromProto(pbPolicy.StoragePolicy)
		if err != nil {
			return aggregation.DefaultID, nil, err
		}
		newAggID, err := aggregation.NewIDFromProto(pbPolicy.AggregationTypes)
		if err != nil {
			return aggregation.DefaultID, nil, err
		}
		if firstTime {
			aggregationID = newAggID
			firstTime = false
		} else if !aggregationID.Equal(newAggID) {
			return aggregation.DefaultID, nil, fmt.Errorf("more than one aggregation ID in legacy policies list proto: ID1=%v, ID2=%v", aggregationID, newAggID)
		}
		storagePoliciesSet[sp] = struct{}{}
	}
	storagePolicies := make([]policy.StoragePolicy, 0, len(storagePoliciesSet))
	for sp := range storagePoliciesSet {
		storagePolicies = append(storagePolicies, sp)
	}
	// NB: for deterministic ordering.
	sort.Sort(policy.ByResolutionAscRetentionDesc(storagePolicies))
	return aggregationID, storagePolicies, nil
}
