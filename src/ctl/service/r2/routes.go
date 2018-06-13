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

package r2

import (
	"fmt"
	"net/http"

	"github.com/m3db/m3metrics/rules/view"

	"github.com/gorilla/mux"
)

func fetchNamespaces(s *service, _ *http.Request) (data interface{}, err error) {
	return s.store.FetchNamespaces()
}

func fetchNamespace(s *service, r *http.Request) (data interface{}, err error) {
	return s.store.FetchRuleSetSnapshot(mux.Vars(r)[namespaceIDVar])
}

func createNamespace(s *service, r *http.Request) (data interface{}, err error) {
	var n view.Namespace
	if err := parseRequest(&n, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	return s.store.CreateNamespace(n.ID, uOpts)
}

func validateRuleSet(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var ruleset view.RuleSet
	if err := parseRequest(&ruleset, r.Body); err != nil {
		return nil, err
	}

	if vars[namespaceIDVar] != ruleset.Namespace {
		return nil, fmt.Errorf(
			"namespaceID param %s and ruleset namespaceID %s do not match",
			vars[namespaceIDVar],
			ruleset.Namespace,
		)
	}

	if err := s.store.ValidateRuleSet(ruleset); err != nil {
		return nil, err
	}

	return "Ruleset is valid", nil
}

func updateRuleSet(s *service, r *http.Request) (data interface{}, err error) {
	var req updateRuleSetRequest
	if err := parseRequest(&req, r.Body); err != nil {
		return nil, NewBadInputError(err.Error())
	}
	if len(req.RuleSetChanges.MappingRuleChanges) == 0 &&
		len(req.RuleSetChanges.RollupRuleChanges) == 0 {
		return nil, NewBadInputError(
			"invalid request: no ruleset changes detected",
		)
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	return s.store.UpdateRuleSet(req.RuleSetChanges, req.RuleSetVersion, uOpts)
}

func deleteNamespace(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	if err := s.store.DeleteNamespace(namespaceID, uOpts); err != nil {
		return nil, err
	}
	return fmt.Sprintf("Deleted namespace %s", namespaceID), nil
}

func fetchMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	return s.store.FetchMappingRule(vars[namespaceIDVar], vars[ruleIDVar])
}

func createMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var mr view.MappingRule
	if err := parseRequest(&mr, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	return s.store.CreateMappingRule(vars[namespaceIDVar], mr, uOpts)
}

func updateMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)

	var mrj view.MappingRule
	if err := parseRequest(&mrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	return s.store.UpdateMappingRule(vars[namespaceIDVar], vars[ruleIDVar], mrj, uOpts)
}

func deleteMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]
	mappingRuleID := vars[ruleIDVar]

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	if err := s.store.DeleteMappingRule(namespaceID, mappingRuleID, uOpts); err != nil {
		return nil, err
	}

	return fmt.Sprintf("Deleted mapping rule: %s in namespace %s", mappingRuleID, namespaceID), nil
}

func fetchMappingRuleHistory(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	snapshots, err := s.store.FetchMappingRuleHistory(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return nil, err
	}
	return view.MappingRuleSnapshots{MappingRules: snapshots}, nil
}

func fetchRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	return s.store.FetchRollupRule(vars[namespaceIDVar], vars[ruleIDVar])
}

func createRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]

	var rrj view.RollupRule
	if err := parseRequest(&rrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	return s.store.CreateRollupRule(namespaceID, rrj, uOpts)
}

func updateRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var rrj view.RollupRule
	if err := parseRequest(&rrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	return s.store.UpdateRollupRule(vars[namespaceIDVar], vars[ruleIDVar], rrj, uOpts)
}

func deleteRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]
	rollupRuleID := vars[ruleIDVar]

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	if err := s.store.DeleteRollupRule(namespaceID, rollupRuleID, uOpts); err != nil {
		return nil, err
	}

	return fmt.Sprintf("Deleted rollup rule: %s in namespace %s", rollupRuleID, namespaceID), nil
}

func fetchRollupRuleHistory(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	snapshots, err := s.store.FetchRollupRuleHistory(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return nil, err
	}
	return view.RollupRuleSnapshots{RollupRules: snapshots}, nil
}
