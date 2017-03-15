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

package policy

import (
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestPoliciesByResolutionAsc(t *testing.T) {
	inputs := []Policy{
		NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
		NewPolicy(10*time.Second, xtime.Second, 2*time.Hour),
		NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
		NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		NewPolicy(time.Minute, xtime.Minute, time.Hour),
		NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
		NewPolicy(10*time.Minute, xtime.Minute, 48*time.Hour),
	}
	expected := []Policy{inputs[2], inputs[0], inputs[1], inputs[5], inputs[4], inputs[3], inputs[6]}
	sort.Sort(ByResolutionAsc(inputs))
	require.Equal(t, expected, inputs)
}

func TestDefaultVersionedPolicies(t *testing.T) {
	var (
		version = 2
		cutover = time.Now()
	)
	vp := DefaultVersionedPolicies(version, cutover)
	require.Equal(t, version, vp.Version)
	require.Equal(t, cutover, vp.Cutover)
	require.True(t, vp.IsDefault())
	require.Equal(t, defaultPolicies, vp.Policies())
}

func TestCustomVersionedPolicies(t *testing.T) {
	var (
		version  = 2
		cutover  = time.Now()
		policies = []Policy{
			NewPolicy(10*time.Second, xtime.Second, 6*time.Hour),
			NewPolicy(10*time.Second, xtime.Second, 2*time.Hour),
		}
	)
	vp := CustomVersionedPolicies(version, cutover, policies)
	require.Equal(t, version, vp.Version)
	require.Equal(t, cutover, vp.Cutover)
	require.False(t, vp.IsDefault())
	require.Equal(t, policies, vp.Policies())
}
