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
)

func BenchmarkVersionedPoliciesAsStruct(b *testing.B) {
	vp := CustomVersionedPolicies(InitPolicyVersion, time.Now(), DefaultPolicies)
	for n := 0; n < b.N; n++ {
		validatePolicyByValue(b, vp)
	}
}

func BenchmarkVersionedPoliciesAsPointer(b *testing.B) {
	vp := CustomVersionedPolicies(InitPolicyVersion, time.Now(), DefaultPolicies)
	for n := 0; n < b.N; n++ {
		validatePolicyByPointer(b, &vp)
	}
}

func BenchmarkVersionedPoliciesAsInterface(b *testing.B) {
	vp := &testVersionedPolicies{version: InitPolicyVersion, cutover: time.Now(), policies: DefaultPolicies}
	for n := 0; n < b.N; n++ {
		validatePolicyByInterface(b, vp)
	}
}

func BenchmarkVersionedPoliciesAsStructExported(b *testing.B) {
	vp := testVersionedPolicies{version: InitPolicyVersion, cutover: time.Now(), policies: DefaultPolicies}
	for n := 0; n < b.N; n++ {
		validatePolicyByStructExported(b, vp)
	}
}

type testVersionedPoliciesInt interface {
	Version() int
}

// VersionedPolicies represent a list of policies at a specified version
type testVersionedPolicies struct {
	// Version is the version of the policies
	version int

	// Cutover is when the policies take effect
	cutover time.Time

	// isDefault determines whether the policies are the default policies
	isDefault bool

	// policies represent the list of policies
	policies []Policy
}

func (v testVersionedPolicies) ValVersion() int {
	return v.version
}

func (v *testVersionedPolicies) Version() int {
	return v.version
}

func validatePolicyByValue(b *testing.B, vps VersionedPolicies) {
	if vps.Version != InitPolicyVersion {
		b.FailNow()
	}
}

func validatePolicyByPointer(b *testing.B, vps *VersionedPolicies) {
	if vps.Version != InitPolicyVersion {
		b.FailNow()
	}
}

func validatePolicyByInterface(b *testing.B, vps testVersionedPoliciesInt) {
	if vps.Version() != InitPolicyVersion {
		b.FailNow()
	}
}

func validatePolicyByStructExported(b *testing.B, vps testVersionedPolicies) {
	if vps.ValVersion() != InitPolicyVersion {
		b.FailNow()
	}
}
