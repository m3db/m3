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

	"github.com/m3db/m3metrics/rules/models"

	"github.com/gorilla/mux"
)

func fetchNamespaces(s *service, _ *http.Request) (data interface{}, err error) {
	view, err := s.store.FetchNamespaces()
	if err != nil {
		return nil, err
	}

	return models.NewNamespaces(view), err
}

func fetchNamespace(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	rs, err := s.store.FetchRuleSet(vars[namespaceIDVar])
	if err != nil {
		return nil, err
	}
	return models.NewRuleSet(rs), nil
}

func createNamespace(s *service, r *http.Request) (data interface{}, err error) {
	var n models.Namespace
	if err := parseRequest(&n, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	view, err := s.store.CreateNamespace(n.ID, uOpts)
	if err != nil {
		return nil, err
	}

	return models.NewNamespace(view), nil
}

func validateRuleSet(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var rsj models.RuleSet
	if err := parseRequest(&rsj, r.Body); err != nil {
		return nil, err
	}
	if vars[namespaceIDVar] != rsj.Namespace {
		return nil, fmt.Errorf(
			"namespaceID param %s and ruleset namespaceID %s do not match",
			vars[namespaceIDVar],
			rsj.Namespace,
		)
	}

	rss, err := rsj.ToRuleSetSnapshotView(models.GenerateID)
	if err != nil {
		return nil, err
	}
	if err := s.store.ValidateRuleSet(rss); err != nil {
		return nil, err
	}
	return "Ruleset is valid", nil
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
	mr, err := s.store.FetchMappingRule(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return nil, err
	}
	return models.NewMappingRule(mr), nil
}

func createMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var mrj models.MappingRule
	if err := parseRequest(&mrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	mr, err := s.store.CreateMappingRule(
		vars[namespaceIDVar],
		mrj.ToMappingRuleView(),
		uOpts,
	)

	if err != nil {
		return nil, err
	}
	return models.NewMappingRule(mr), nil
}

func updateMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)

	var mrj models.MappingRule
	if err := parseRequest(&mrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	mr, err := s.store.UpdateMappingRule(
		vars[namespaceIDVar],
		vars[ruleIDVar],
		mrj.ToMappingRuleView(),
		uOpts,
	)

	if err != nil {
		return nil, err
	}

	return models.NewMappingRule(mr), nil
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
	hist, err := s.store.FetchMappingRuleHistory(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return nil, err
	}
	return models.NewMappingRuleSnapshots(hist), nil
}

func fetchRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	rr, err := s.store.FetchRollupRule(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return nil, err
	}
	return models.NewRollupRule(rr), nil
}

func createRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]

	var rrj models.RollupRule
	if err := parseRequest(&rrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	rr, err := s.store.CreateRollupRule(namespaceID, rrj.ToRollupRuleView(), uOpts)
	if err != nil {
		return nil, err
	}
	return models.NewRollupRule(rr), nil
}

func updateRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var rrj models.RollupRule
	if err := parseRequest(&rrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	rr, err := s.store.UpdateRollupRule(
		vars[namespaceIDVar],
		vars[ruleIDVar],
		rrj.ToRollupRuleView(),
		uOpts,
	)

	if err != nil {
		return nil, err
	}
	return models.NewRollupRule(rr), nil
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
	hist, err := s.store.FetchRollupRuleHistory(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return nil, err
	}
	return models.NewRollupRuleSnapshots(hist), nil
}
