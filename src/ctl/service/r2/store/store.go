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
// THE SOFTWARE

package store

import (
	"github.com/m3db/m3metrics/rules/view"
	"github.com/m3db/m3metrics/rules/view/changes"
)

// Store is a construct that can perform operations against a backing rule store.
type Store interface {
	// FetchNamespaces fetches namespaces.
	FetchNamespaces() (view.Namespaces, error)

	// CreateNamespace creates a namespace for the given namespace ID.
	CreateNamespace(namespaceID string, uOpts UpdateOptions) (view.Namespace, error)

	// DeleteNamespace deletes the namespace for the given namespace ID.
	DeleteNamespace(namespaceID string, uOpts UpdateOptions) error

	// FetchRuleSetSnapshot fetches the latest ruleset snapshot for the given namespace ID.
	FetchRuleSetSnapshot(namespaceID string) (view.RuleSet, error)

	// ValidateRuleSet validates a namespace's ruleset.
	ValidateRuleSet(rs view.RuleSet) error

	// UpdateRuleSet updates a ruleset with a given namespace.
	UpdateRuleSet(rsChanges changes.RuleSetChanges, version int, uOpts UpdateOptions) (view.RuleSet, error)

	// FetchMappingRule fetches the mapping rule for the given namespace ID and rule ID.
	FetchMappingRule(namespaceID, mappingRuleID string) (view.MappingRule, error)

	// CreateMappingRule creates a mapping rule for the given namespace ID and rule data.
	CreateMappingRule(namespaceID string, mrv view.MappingRule, uOpts UpdateOptions) (view.MappingRule, error)

	// UpdateMappingRule updates a mapping rule for the given namespace ID and rule data.
	UpdateMappingRule(namespaceID, mappingRuleID string, mrv view.MappingRule, uOpts UpdateOptions) (view.MappingRule, error)

	// DeleteMappingRule deletes the mapping rule for the given namespace ID and rule ID.
	DeleteMappingRule(namespaceID, mappingRuleID string, uOpts UpdateOptions) error

	// FetchMappingRuleHistory fetches the history of the mapping rule for the given namespace ID
	// and rule ID.
	FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]view.MappingRule, error)

	// FetchRollupRule fetches the rollup rule for the given namespace ID and rule ID.
	FetchRollupRule(namespaceID, rollupRuleID string) (view.RollupRule, error)

	// CreateRollupRule creates a rollup rule for the given namespace ID and rule data.
	CreateRollupRule(namespaceID string, rrv view.RollupRule, uOpts UpdateOptions) (view.RollupRule, error)

	// UpdateRollupRule updates a rollup rule for the given namespace ID and rule data.
	UpdateRollupRule(namespaceID, rollupRuleID string, rrv view.RollupRule, uOpts UpdateOptions) (view.RollupRule, error)

	// DeleteRollupRule deletes the rollup rule for the given namespace ID and rule ID.
	DeleteRollupRule(namespaceID, rollupRuleID string, uOpts UpdateOptions) error

	// FetchRollupRuleHistory fetches the history of the rollup rule for the given namespace ID
	// and rule ID.
	FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]view.RollupRule, error)

	// Close closes the store.
	Close()
}
