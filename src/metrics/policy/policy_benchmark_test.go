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

package policy

import (
	"testing"
	"time"

	"github.com/m3db/m3x/time"
)

var (
	testNowNanos = time.Now().UnixNano()
	testPolicies = []Policy{
		NewPolicy(NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), DefaultAggregationID),
		NewPolicy(NewStoragePolicy(time.Minute, xtime.Minute, 30*24*time.Hour), DefaultAggregationID),
	}
)

func BenchmarkStagedPoliciesAsStruct(b *testing.B) {
	sp := NewStagedPolicies(testNowNanos, false, testPolicies)
	for n := 0; n < b.N; n++ {
		validatePolicyByValue(b, sp)
	}
}

func BenchmarkStagedPoliciesAsPointer(b *testing.B) {
	sp := NewStagedPolicies(testNowNanos, false, testPolicies)
	for n := 0; n < b.N; n++ {
		validatePolicyByPointer(b, &sp)
	}
}

func BenchmarkStagedPoliciesAsInterface(b *testing.B) {
	sp := &testStagedPolicies{cutoverNanos: testNowNanos, policies: testPolicies}
	for n := 0; n < b.N; n++ {
		validatePolicyByInterface(b, sp)
	}
}

func BenchmarkStagedPoliciesAsStructExported(b *testing.B) {
	sp := testStagedPolicies{cutoverNanos: testNowNanos, policies: testPolicies}
	for n := 0; n < b.N; n++ {
		validatePolicyByStructExported(b, sp)
	}
}

type testStagedPoliciesInt64 interface {
	CutoverNanos() int64
}

// StagedPolicies represent a list of policies at a specified version.
type testStagedPolicies struct {
	cutoverNanos int64
	policies     []Policy
}

func (v testStagedPolicies) ValCutoverNanos() int64 {
	return v.cutoverNanos
}

func (v *testStagedPolicies) CutoverNanos() int64 {
	return v.cutoverNanos
}

func validatePolicyByValue(b *testing.B, sp StagedPolicies) {
	if sp.CutoverNanos != testNowNanos {
		b.FailNow()
	}
}

func validatePolicyByPointer(b *testing.B, sp *StagedPolicies) {
	if sp.CutoverNanos != testNowNanos {
		b.FailNow()
	}
}

func validatePolicyByInterface(b *testing.B, sp testStagedPoliciesInt64) {
	if sp.CutoverNanos() != testNowNanos {
		b.FailNow()
	}
}

func validatePolicyByStructExported(b *testing.B, sp testStagedPolicies) {
	if sp.ValCutoverNanos() != testNowNanos {
		b.FailNow()
	}
}
