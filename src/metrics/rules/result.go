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
	"time"

	"github.com/m3db/m3metrics/policy"
)

var (
	// EmptyMatchResult is the result when no matches were found.
	EmptyMatchResult = NewMatchResult(0, 0, timeNsMax, nil, nil)
)

// RollupResult contains the rollup metric id and the associated policies.
type RollupResult struct {
	ID       []byte
	Policies []policy.Policy
}

// MatchResult represents a match result.
type MatchResult struct {
	version    int
	cutoverNs  int64
	expireAtNs int64
	mappings   []policy.Policy
	rollups    []RollupResult
}

// NewMatchResult creates a new match result.
func NewMatchResult(
	version int,
	cutoverNs int64,
	expireAtNs int64,
	mappings []policy.Policy,
	rollups []RollupResult,
) MatchResult {
	return MatchResult{
		version:    version,
		cutoverNs:  cutoverNs,
		expireAtNs: expireAtNs,
		mappings:   mappings,
		rollups:    rollups,
	}
}

// HasExpired returns whether the match result has expired for a given time.
func (r *MatchResult) HasExpired(t time.Time) bool { return r.expireAtNs <= t.UnixNano() }

// NumRollups returns the number of rollup result associated with the given id.
func (r *MatchResult) NumRollups() int { return len(r.rollups) }

// Mappings returns the mapping policies for the given id.
func (r *MatchResult) Mappings() policy.VersionedPolicies {
	return r.versionedPolicies(r.mappings)
}

// Rollups returns the rollup metric id and corresponding policies at a given index.
func (r *MatchResult) Rollups(idx int) ([]byte, policy.VersionedPolicies) {
	rollup := r.rollups[idx]
	return rollup.ID, r.versionedPolicies(rollup.Policies)
}

// TODO(xichen): change versioned policies to use int64 instead.
func (r *MatchResult) versionedPolicies(policies []policy.Policy) policy.VersionedPolicies {
	// NB(xichen): if there are no policies for this id, we fall
	// back to the default mapping policies.
	if len(policies) == 0 {
		return policy.DefaultVersionedPolicies(r.version, time.Unix(0, r.cutoverNs))
	}
	return policy.CustomVersionedPolicies(r.version, time.Unix(0, r.cutoverNs), policies)
}
