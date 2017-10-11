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
// THE SOFTWARE

package r2

import "github.com/m3db/m3metrics/rules"

// Store is a construct that can perform operations against a backing rule store.
type Store interface {
	// Fetch namespaces and their version
	FetchNamespaces() (*rules.NamespacesView, error)

	CreateNamespace(namespaceID string, uOpts UpdateOptions) (*rules.NamespaceView, error)

	DeleteNamespace(namespaceID string, uOpts UpdateOptions) error

	FetchRuleSet(namespaceID string) (*rules.RuleSetSnapshot, error)

	FetchMappingRule(namespaceID, mappingRuleID string) (*rules.MappingRuleView, error)

	CreateMappingRule(namespaceID string, mrv *rules.MappingRuleView, uOpts UpdateOptions) (*rules.MappingRuleView, error)

	UpdateMappingRule(namespaceID, mappingRuleID string, mrv *rules.MappingRuleView, uOpts UpdateOptions) (*rules.MappingRuleView, error)

	DeleteMappingRule(namespaceID, mappingRuleID string, uOpts UpdateOptions) error

	FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]*rules.MappingRuleView, error)

	FetchRollupRule(namespaceID, rollupRuleID string) (*rules.RollupRuleView, error)

	CreateRollupRule(namespaceID string, rrv *rules.RollupRuleView, uOpts UpdateOptions) (*rules.RollupRuleView, error)

	UpdateRollupRule(namespaceID, rollupRuleID string, rrv *rules.RollupRuleView, uOpts UpdateOptions) (*rules.RollupRuleView, error)

	DeleteRollupRule(namespaceID, rollupRuleID string, uOpts UpdateOptions) error

	FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]*rules.RollupRuleView, error)
}
