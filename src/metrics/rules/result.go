// Copyright (c) 2017 Uber Technologies, Inc.
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
	"bytes"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/policy"
)

var (
	// EmptyMatchResult is the result when no matches were found.
	EmptyMatchResult  = NewMatchResult(kv.UninitializedVersion, timeNanosMax, policy.DefaultPoliciesList, nil)
	emptyRollupResult RollupResult
)

// RollupResult contains the rollup metric id and the associated policies list.
type RollupResult struct {
	ID           []byte
	PoliciesList policy.PoliciesList
}

// RollupResultsByIDAsc sorts rollup results by id in ascending order.
type RollupResultsByIDAsc []RollupResult

func (a RollupResultsByIDAsc) Len() int           { return len(a) }
func (a RollupResultsByIDAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a RollupResultsByIDAsc) Less(i, j int) bool { return bytes.Compare(a[i].ID, a[j].ID) < 0 }

// MatchResult represents a match result.
type MatchResult struct {
	version       int
	expireAtNanos int64
	mappings      policy.PoliciesList
	rollups       []RollupResult
}

// NewMatchResult creates a new match result.
func NewMatchResult(
	version int,
	expireAtNanos int64,
	mappings policy.PoliciesList,
	rollups []RollupResult,
) MatchResult {
	return MatchResult{
		version:       version,
		expireAtNanos: expireAtNanos,
		mappings:      mappings,
		rollups:       rollups,
	}
}

// Version returns the version of the match result.
func (r *MatchResult) Version() int { return r.version }

// HasExpired returns whether the match result has expired for a given time.
func (r *MatchResult) HasExpired(timeNanos int64) bool { return r.expireAtNanos <= timeNanos }

// NumRollups returns the number of rollup metrics.
func (r *MatchResult) NumRollups() int { return len(r.rollups) }

// MappingsAt returns the active mapping policies at a given time.
func (r *MatchResult) MappingsAt(timeNanos int64) policy.PoliciesList {
	return activePoliciesAt(r.mappings, timeNanos)
}

// RollupsAt returns the rollup result at a given index and time, along with a boolean
// flag that indicates whether the rollup metric has been tombstoned.
func (r *MatchResult) RollupsAt(idx int, timeNanos int64) (RollupResult, bool) {
	rollup := r.rollups[idx]
	policiesList := activePoliciesAt(rollup.PoliciesList, timeNanos)
	numPolicies := len(policiesList)
	if numPolicies > 0 {
		lastStagedPolicies := policiesList[numPolicies-1]
		if lastStagedPolicies.Tombstoned && lastStagedPolicies.CutoverNanos <= timeNanos {
			return emptyRollupResult, true
		}
	}
	return RollupResult{
		ID:           rollup.ID,
		PoliciesList: policiesList,
	}, false
}

// activePolicies returns the active policies at a given time, assuming
// the input policies are sorted by cutover time in ascending order.
func activePoliciesAt(policies policy.PoliciesList, timeNanos int64) policy.PoliciesList {
	for idx := len(policies) - 1; idx >= 0; idx-- {
		if policies[idx].CutoverNanos <= timeNanos {
			return policies[idx:]
		}
	}
	return policies
}
