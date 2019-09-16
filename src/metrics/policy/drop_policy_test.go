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

package policy

import (
	"fmt"
	"math"
	"testing"

	"github.com/m3db/m3/src/x/test/testmarshal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func TestDropPolicyIsDefault(t *testing.T) {
	assert.True(t, DefaultDropPolicy.IsDefault())
}

func TestDropPolicyString(t *testing.T) {
	for _, policy := range validDropPolicies {
		switch policy {
		case DropNone:
			assert.Equal(t, "drop_none", policy.String())
		case DropMust:
			assert.Equal(t, "drop_must", policy.String())
		case DropIfOnlyMatch:
			assert.Equal(t, "drop_if_only_match", policy.String())
		default:
			assert.Fail(t, fmt.Sprintf("unknown policy: value=%d, string=%s",
				int(policy), policy.String()))
		}
	}
}

func TestDropPolicyIsValid(t *testing.T) {
	for _, policy := range validDropPolicies {
		assert.True(t, policy.IsValid())
	}
	assert.False(t, DropPolicy(math.MaxUint64).IsValid())
}

func TestDropPolicyMarshalling(t *testing.T) {
	inputs := []struct {
		str      string
		expected DropPolicy
	}{
		{
			str:      "",
			expected: DropNone,
		},
		{
			str:      DropNone.String(),
			expected: DropNone,
		},
		{
			str:      DropMust.String(),
			expected: DropMust,
		},
		{
			str:      DropIfOnlyMatch.String(),
			expected: DropIfOnlyMatch,
		},
	}

	examples := make([]DropPolicy, 0, len(inputs))
	for _, p := range inputs {
		examples = append(examples, p.expected)
	}

	t.Run("roundtrips", func(t *testing.T) {
		testmarshal.TestMarshalersRoundtrip(t, examples, []testmarshal.Marshaler{testmarshal.YAMLMarshaler, testmarshal.JSONMarshaler, testmarshal.TextMarshaler})
	})

	t.Run("yaml/unmarshals", func(t *testing.T) {
		for _, input := range inputs {
			testmarshal.Require(t, testmarshal.AssertUnmarshals(t, testmarshal.YAMLMarshaler, input.expected, []byte(input.str)))
		}
	})
}

func TestDropPolicyUnmarshalYAMLErrors(t *testing.T) {
	inputs := []string{
		"drop_musty",
		"drop_unknown",
	}
	for _, input := range inputs {
		var p DropPolicy
		require.Error(t, yaml.Unmarshal([]byte(input), &p))
	}
}
