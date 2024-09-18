// Copyright (c) 2024 Uber Technologies, Inc.
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
	"errors"
	"fmt"
	"strings"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/metrics/generated/proto/attributionpb"
	"github.com/m3db/m3/src/metrics/tagfiltertree"
)

var (
	errEmptyName = errors.New("invalid rule: empty name")
)

// RuleSet provides accessibility to the ruleSet.
type RuleSet interface {
	// Name returns the name of the ruleSet.
	Name() string

	// SetName sets the name of the ruleSet.
	SetName(value string) RuleSet

	// Rules returns the ruleSet.
	Rules() []*Rule

	// SetRules sets the ruleSet.
	SetRules(rules []*Rule) RuleSet

	// Version returns the version of the ruleSet.
	Version() int

	// SetVersion sets the version of the rule.
	SetVersion(value int) RuleSet

	// Validate validates the ruleSet.
	Validate() error
}

type ruleSet struct {
	// name is the key of the ruleSet in ETCD.
	name    string
	rules   []*Rule
	version int
}

type Rule struct {
	name        string
	primaryNS   string
	secondaryNS string
	tagFilters  []string
	uOwnID      string
}

// NewRule creates a new rule.
func NewRuleSet() RuleSet { return new(ruleSet) }

// NewRuleSetFromValue creates a rule from a kv.Value.
func NewRuleSetFromValue(v kv.Value) (RuleSet, error) {
	var ruleSet attributionpb.RuleSet
	if err := v.Unmarshal(&ruleSet); err != nil {
		return nil, err
	}
	t, err := NewRuleSetFromProto(&ruleSet)
	if err != nil {
		return nil, err
	}
	return t.SetVersion(v.Version()), nil
}

// NewRuleSetFromProto creates a rule from a proto.
func NewRuleSetFromProto(r *attributionpb.RuleSet) (RuleSet, error) {
	rs := make([]*Rule, len(r.Rules))
	for i, rulepb := range r.Rules {
		r, err := NewRuleFromProto(rulepb)
		if err != nil {
			return nil, err
		}
		rs[i] = r
	}
	return NewRuleSet().
		SetRules(rs), nil
}

func (rs *ruleSet) Name() string {
	return rs.name
}

func (rs *ruleSet) SetName(value string) RuleSet {
	rs.name = value
	return rs
}

func (rs *ruleSet) Rules() []*Rule {
	return rs.rules
}

func (rs *ruleSet) SetRules(rules []*Rule) RuleSet {
	newt := *rs
	newt.rules = rules
	return &newt
}

func (rs *ruleSet) Version() int {
	return rs.version
}

func (rs *ruleSet) SetVersion(value int) RuleSet {
	newt := *rs
	newt.version = value
	return &newt
}

func (rs *ruleSet) String() string {
	var buf bytes.Buffer
	buf.WriteString("\n{\n")
	buf.WriteString(fmt.Sprintf("\tversion: %d\n", rs.version))
	buf.WriteString(fmt.Sprintf("\trules:\n"))
	for _, r := range rs.rules {
		buf.WriteString(fmt.Sprintf("\t\t%v\n", r.String()))
	}
	buf.WriteString("}\n")
	return buf.String()
}

func (rs *ruleSet) Validate() error {
	for _, r := range rs.rules {
		if err := r.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// NewRuleSetToProto creates proto from a ruleSet.
func NewRuleSetToProto(rs RuleSet) (*attributionpb.RuleSet, error) {
	rules := make([]*attributionpb.Rule, len(rs.Rules()))
	for i, r := range rs.Rules() {
		rpb, err := NewRuleToProto(r)
		if err != nil {
			return nil, err
		}
		rules[i] = rpb
	}
	return &attributionpb.RuleSet{
		Rules: rules,
	}, nil
}

func (r *Rule) Validate() error {
	if r.Name() == "" {
		return errEmptyName
	}
	return nil
}

// NewRule creates a new rule.
func NewRule() *Rule { return new(Rule) }

func (r *Rule) Name() string {
	return r.name
}

func (r *Rule) SetName(value string) *Rule {
	newr := *r
	newr.name = value
	return &newr
}

func (r *Rule) PrimaryNS() string {
	return r.primaryNS
}

func (r *Rule) SetPrimaryNS(value string) *Rule {
	newr := *r
	newr.primaryNS = value
	return &newr
}

func (r *Rule) SecondaryNS() string {
	return r.secondaryNS
}

func (r *Rule) SetSecondaryNS(value string) *Rule {
	newr := *r
	newr.secondaryNS = value
	return &newr
}

func (r *Rule) TagFilters() []string {
	return r.tagFilters
}

func (r *Rule) SetTagFilters(value []string) *Rule {
	newr := *r
	newr.tagFilters = value
	return &newr
}

func (r *Rule) UOwnID() string {
	return r.uOwnID
}

func (r *Rule) SetUOwnID(value string) *Rule {
	newr := *r
	newr.uOwnID = value
	return &newr
}

func (r *Rule) String() string {
	var buf bytes.Buffer
	buf.WriteString("\n{\n")
	buf.WriteString(fmt.Sprintf("\tname: %s\n", r.name))
	buf.WriteString(fmt.Sprintf("\tnamespace: %s\n", r.primaryNS))
	buf.WriteString(fmt.Sprintf("\tnamespace: %s\n", r.secondaryNS))
	buf.WriteString(fmt.Sprintf("\ttagFilters: %v\n", r.tagFilters))
	buf.WriteString(fmt.Sprintf("\tuOwnID: %s\n", r.uOwnID))
	buf.WriteString("}\n")
	return buf.String()
}

// NewRuleToProto creates proto from a rule.
func NewRuleToProto(r *Rule) (*attributionpb.Rule, error) {
	return &attributionpb.Rule{
		Name:        r.Name(),
		PrimaryNs:   r.PrimaryNS(),
		SecondaryNs: r.SecondaryNS(),
		TagFilters:  r.TagFilters(),
		UownId:      r.UOwnID(),
	}, nil
}

func NewRuleFromProto(rpb *attributionpb.Rule) (*Rule, error) {
	return NewRule().
		SetName(rpb.GetName()).
		SetPrimaryNS(rpb.GetPrimaryNs()).
		SetSecondaryNS(rpb.GetSecondaryNs()).
		SetTagFilters(rpb.GetTagFilters()).
		SetUOwnID(rpb.GetUownId()), nil
}

type Result struct {
	Rule        *Rule
	PrimaryNS   string
	SecondaryNS string
}

func (r *Rule) Resolve(inputTags map[string]string) (Result, error) {
	if !tagfiltertree.IsVarTagValue(r.PrimaryNS()) &&
		!tagfiltertree.IsVarTagValue(r.SecondaryNS()) {
		return Result{
			Rule:        r,
			PrimaryNS:   r.PrimaryNS(),
			SecondaryNS: r.SecondaryNS(),
		}, nil
	}

	// resolve the var tags.
	varMap := make(map[string]string)
	for _, tagFilter := range r.tagFilters {
		tags, err := tagfiltertree.TagsFromTagFilter(tagFilter)
		if err != nil {
			return Result{}, err
		}
		for _, tag := range tags {
			if tag.Var != "" {
				// is var tag
				inputTagValue, ok := inputTags[tag.Name]
				if !ok {
					return Result{}, errors.New("tag not found")
				}
				varMap[tag.Var] = inputTagValue
			}
		}
	}

	primaryNS := r.PrimaryNS()
	for varName, varValue := range varMap {
		primaryNS = strings.ReplaceAll(primaryNS, varName, varValue)
	}

	secondaryNS := r.SecondaryNS()
	for varName, varValue := range varMap {
		secondaryNS = strings.ReplaceAll(secondaryNS, varName, varValue)
	}

	return Result{
		Rule:        r,
		PrimaryNS:   primaryNS,
		SecondaryNS: secondaryNS,
	}, nil
}
