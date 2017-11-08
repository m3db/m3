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
	"time"

	"github.com/gorilla/mux"
)

func fetchNamespaces(s *service, _ *http.Request) (data interface{}, err error) {
	view, err := s.store.FetchNamespaces()
	if err != nil {
		return nil, err
	}

	return newNamespacesJSON(view), err
}

func fetchNamespace(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	rs, err := s.store.FetchRuleSet(vars[namespaceIDVar])
	if err != nil {
		return nil, err
	}
	return newRuleSetJSON(rs), nil
}

func createNamespace(s *service, r *http.Request) (data interface{}, err error) {
	var n namespaceJSON
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

	return newNamespaceJSON(view), nil
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
	return newMappingRuleJSON(mr), nil
}

func createMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var mrj mappingRuleJSON
	if err := parseRequest(&mrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	mr, err := s.store.CreateMappingRule(
		vars[namespaceIDVar],
		mrj.mappingRuleView(),
		uOpts,
	)

	if err != nil {
		return nil, err
	}
	return newMappingRuleJSON(mr), nil
}

func updateMappingRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)

	var mrj mappingRuleJSON
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
		mrj.mappingRuleView(),
		uOpts,
	)

	if err != nil {
		return nil, err
	}

	return newMappingRuleJSON(mr), nil
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
	return newMappingRuleHistoryJSON(hist), nil
}

func fetchRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	rr, err := s.store.FetchRollupRule(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return nil, err
	}
	return newRollupRuleJSON(rr), nil
}

func createRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]

	var rrj rollupRuleJSON
	if err := parseRequest(&rrj, r.Body); err != nil {
		return nil, err
	}

	uOpts, err := s.newUpdateOptions(r)
	if err != nil {
		return nil, err
	}

	rr, err := s.store.CreateRollupRule(namespaceID, rrj.rollupRuleView(), uOpts)
	if err != nil {
		return nil, err
	}
	return newRollupRuleJSON(rr), nil
}

func updateRollupRule(s *service, r *http.Request) (data interface{}, err error) {
	vars := mux.Vars(r)
	var rrj rollupRuleJSON
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
		rrj.rollupRuleView(),
		uOpts,
	)

	if err != nil {
		return nil, err
	}
	return newRollupRuleJSON(rr), nil
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
	return newRollupRuleHistoryJSON(hist), nil
}

type routeFunc func(s *service, r *http.Request) (data interface{}, err error)

func handleRoute(rf routeFunc, s *service, r *http.Request, namespace string) (interface{}, error) {
	start := s.nowFn()
	data, err := rf(s, r)
	s.metrics.recordMetric(r.RequestURI, r.Method, namespace, time.Since(start), err)
	if err != nil {
		return nil, err
	}
	return data, nil
}
