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

package handlers

import (
	"errors"
	"fmt"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
)

var (
	errMultipleMatches = errors.New("more than one match found")
)

// Rule returns the rule with a given name, or an error if there are mutliple matches
func Rule(ruleSet *schema.RuleSet, ruleName string) (*schema.MappingRule, *schema.RollupRule, error) {
	var (
		mappingRule *schema.MappingRule
		rollupRule  *schema.RollupRule
	)
	for _, mr := range ruleSet.MappingRules {
		if len(mr.Snapshots) == 0 {
			continue
		}

		latestSnapshot := mr.Snapshots[len(mr.Snapshots)-1]
		name := latestSnapshot.Name
		if name != ruleName || latestSnapshot.Tombstoned {
			continue
		}
		if mappingRule == nil {
			mappingRule = mr
		} else {
			return nil, nil, errMultipleMatches
		}
	}
	for _, rr := range ruleSet.RollupRules {
		if len(rr.Snapshots) == 0 {
			continue
		}
		latestSnapshot := rr.Snapshots[len(rr.Snapshots)-1]
		name := latestSnapshot.Name
		if name != ruleName || latestSnapshot.Tombstoned {
			continue
		}
		if rollupRule == nil {
			rollupRule = rr
		} else {
			return nil, nil, errMultipleMatches
		}
	}
	if mappingRule != nil && rollupRule != nil {
		return nil, nil, errMultipleMatches
	}

	if mappingRule == nil && rollupRule == nil {
		return nil, nil, kv.ErrNotFound
	}
	return mappingRule, rollupRule, nil
}

// RuleSet returns the version and the persisted ruleset data in kv store.
func RuleSet(store kv.Store, ruleSetKey string) (int, *schema.RuleSet, error) {
	value, err := store.Get(ruleSetKey)
	if err != nil {
		return 0, nil, err
	}
	version := value.Version()
	var ruleSet schema.RuleSet
	if err := value.Unmarshal(&ruleSet); err != nil {
		return 0, nil, err
	}

	return version, &ruleSet, nil
}

// RuleSetKey returns the ruleset key given the namespace name.
func RuleSetKey(keyFmt string, namespace string) string {
	return fmt.Sprintf(keyFmt, namespace)
}

// ValidateRuleSet validates that a valid RuleSet exists in that keyspace.
func ValidateRuleSet(store kv.Store, ruleSetKey string) (int, *schema.RuleSet, error) {
	ruleSetVersion, ruleSet, err := RuleSet(store, ruleSetKey)
	if err != nil {
		return 0, nil, fmt.Errorf("could not read ruleSet data for key %s: %v", ruleSetKey, err)
	}
	if ruleSet.Tombstoned {
		return 0, nil, fmt.Errorf("ruleset %s is tombstoned", ruleSetKey)
	}
	return ruleSetVersion, ruleSet, nil
}
