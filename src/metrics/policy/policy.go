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
	"errors"
	"strconv"
	"strings"

	"github.com/m3db/m3metrics/generated/proto/schema"
)

const (
	policyAggregationTypeSeparator = "|"
)

var (
	// DefaultPolicy represents a default policy.
	DefaultPolicy Policy

	errNilPolicySchema     = errors.New("nil policy schema")
	errInvalidPolicyString = errors.New("invalid policy string")
)

// Policy contains a storage policy and a list of custom aggregation types.
type Policy struct {
	StoragePolicy
	AggregationID
}

// NewPolicy creates a policy.
func NewPolicy(sp StoragePolicy, aggTypes AggregationID) Policy {
	return Policy{StoragePolicy: sp, AggregationID: aggTypes}
}

// NewPolicyFromSchema creates a new policy from a schema policy.
func NewPolicyFromSchema(p *schema.Policy) (Policy, error) {
	if p == nil {
		return DefaultPolicy, errNilPolicySchema
	}

	policy, err := NewStoragePolicyFromSchema(p.StoragePolicy)
	if err != nil {
		return DefaultPolicy, err
	}

	aggID, err := NewAggregationIDFromSchema(p.AggregationTypes)
	if err != nil {
		return DefaultPolicy, err
	}

	return NewPolicy(policy, aggID), nil

}

// Schema returns the schema of the policy.
func (p Policy) Schema() (*schema.Policy, error) {
	sp, err := p.StoragePolicy.Schema()
	if err != nil {
		return nil, err
	}

	aggTypes, err := NewAggregationIDDecompressor().Decompress(p.AggregationID)
	if err != nil {
		return nil, err
	}

	schemaAggTypes, err := aggTypes.Schema()
	if err != nil {
		return nil, err
	}

	return &schema.Policy{
		StoragePolicy:    sp,
		AggregationTypes: schemaAggTypes,
	}, nil
}

// String is the string representation of a policy.
func (p Policy) String() string {
	if p.AggregationID.IsDefault() {
		return p.StoragePolicy.String()
	}
	return p.StoragePolicy.String() + policyAggregationTypeSeparator + p.AggregationID.String()
}

// MarshalJSON returns the JSON encoding of a policy.
func (p Policy) MarshalJSON() ([]byte, error) {
	marshalled := strconv.Quote(p.String())
	return []byte(marshalled), nil
}

// UnmarshalJSON unmarshals JSON-encoded data into staged a policy.
func (p *Policy) UnmarshalJSON(data []byte) error {
	str := string(data)
	unquoted, err := strconv.Unquote(str)
	if err != nil {
		return err
	}
	parsed, err := ParsePolicy(unquoted)
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}

// UnmarshalYAML unmarshals a policy value from a string.
func (p *Policy) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	parsed, err := ParsePolicy(str)
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}

// ParsePolicy parses a policy in the form of resolution:retention|aggregationTypes.
func ParsePolicy(str string) (Policy, error) {
	parts := strings.Split(str, policyAggregationTypeSeparator)
	l := len(parts)
	if l > 2 {
		return DefaultPolicy, errInvalidPolicyString
	}

	p, err := ParseStoragePolicy(parts[0])
	if err != nil {
		return DefaultPolicy, err
	}

	var id = DefaultAggregationID
	if l == 2 {
		aggTypes, err := ParseAggregationTypes(parts[1])
		if err != nil {
			return DefaultPolicy, err
		}

		id, err = NewAggregationIDCompressor().Compress(aggTypes)
		if err != nil {
			return DefaultPolicy, err
		}
	}

	return NewPolicy(p, id), nil
}

// NewPoliciesFromSchema creates multiple new policies from given schema policies.
func NewPoliciesFromSchema(policies []*schema.Policy) ([]Policy, error) {
	res := make([]Policy, 0, len(policies))
	for _, p := range policies {
		policy, err := NewPolicyFromSchema(p)
		if err != nil {
			return nil, err
		}
		res = append(res, policy)
	}
	return res, nil
}

// ByResolutionAsc implements the sort.Sort interface to sort
// policies by resolution in ascending order, finest resolution first.
// If two policies have the same resolution, the one with longer
// retention period comes first.
type ByResolutionAsc []Policy

func (pr ByResolutionAsc) Len() int      { return len(pr) }
func (pr ByResolutionAsc) Swap(i, j int) { pr[i], pr[j] = pr[j], pr[i] }

func (pr ByResolutionAsc) Less(i, j int) bool {
	p1, p2 := pr[i], pr[j]
	sp1, sp2 := p1.StoragePolicy, p2.StoragePolicy
	rw1, rw2 := sp1.Resolution().Window, sp2.Resolution().Window
	if rw1 < rw2 {
		return true
	}
	if rw1 > rw2 {
		return false
	}
	r1, r2 := sp1.Retention(), sp2.Retention()
	if r1 > r2 {
		return true
	}
	if r1 < r2 {
		return false
	}
	rp1, rp2 := sp1.Resolution().Precision, sp2.Resolution().Precision
	if rp1 < rp2 {
		return true
	}
	if rp1 > rp2 {
		return false
	}
	at1, at2 := p1.AggregationID, p2.AggregationID
	for k := 0; k < AggregationIDLen; k++ {
		if at1[k] < at2[k] {
			return true
		}
		if at1[k] > at2[k] {
			return false
		}
	}
	// If everything equals, prefer the first one
	return true
}
