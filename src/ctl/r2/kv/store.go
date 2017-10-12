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

package kv

import (
	"fmt"

	"github.com/m3db/m3ctl/r2"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
)

type store struct {
	nowFn        clock.NowFn
	opts         StoreOptions
	ruleStore    rules.Store
	updateHelper rules.RuleSetUpdateHelper
}

// NewStore returns a new service that knows how to talk to a kv backed r2 store.
func NewStore(rs rules.Store, opts StoreOptions) r2.Store {
	clockOpts := opts.ClockOptions()
	updateHelper := rules.NewRuleSetUpdateHelper(opts.RuleUpdatePropagationDelay())
	return &store{
		nowFn:        clockOpts.NowFn(),
		opts:         opts,
		ruleStore:    rs,
		updateHelper: updateHelper,
	}
}

func (s *store) FetchNamespaces() (*rules.NamespacesView, error) {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	namespaces, err := nss.NamespacesView()
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	liveNss := make([]*rules.NamespaceView, 0, len(namespaces.Namespaces))
	for _, ns := range namespaces.Namespaces {
		if !ns.Tombstoned {
			liveNss = append(liveNss, ns)
		}
	}

	return &rules.NamespacesView{
		Namespaces: liveNss,
		Version:    namespaces.Version,
	}, nil
}

func (s *store) CreateNamespace(namespaceID string, uOpts r2.UpdateOptions) (*rules.NamespaceView, error) {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	revived, err := nss.AddNamespace(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	um := s.newUpdateMeta(uOpts)
	rs := rules.NewEmptyRuleSet(namespaceID, um)

	if revived {
		rawRs, err := s.ruleStore.ReadRuleSet(namespaceID)
		if err != nil {
			return nil, s.handleUpstreamError(err)
		}
		rs = rawRs.ToMutableRuleSet().Clone()
		if err = rs.Revive(um); err != nil {
			return nil, s.handleUpstreamError(err)
		}
	}

	if err = s.ruleStore.WriteAll(nss, rs); err != nil {
		return nil, s.handleUpstreamError(err)
	}

	nss, err = s.ruleStore.ReadNamespaces()
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	ns, err := nss.Namespace(namespaceID)
	if err != nil {
		return nil, r2.NewNotFoundError(fmt.Sprintf("namespace: %s does not exist", namespaceID))
	}

	// Get the latest view of the namespace.
	view, err := ns.NamespaceView(len(ns.Snapshots()) - 1)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	return view, nil
}

func (s *store) DeleteNamespace(namespaceID string, uOpts r2.UpdateOptions) error {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return s.handleUpstreamError(err)
	}

	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return s.handleUpstreamError(err)
	}

	err = nss.DeleteNamespace(namespaceID, rs.Version())
	if err != nil {
		return s.handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.Delete(s.newUpdateMeta(uOpts))
	if err != nil {
		return s.handleUpstreamError(err)
	}

	if err = s.ruleStore.WriteAll(nss, mutable); err != nil {
		return s.handleUpstreamError(err)
	}

	return nil
}

func (s *store) FetchRuleSet(namespaceID string) (*rules.RuleSetSnapshot, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}
	return rs.Latest()
}

func (s *store) FetchMappingRule(namespaceID string, mappingRuleID string) (*rules.MappingRuleView, error) {
	crs, err := s.FetchRuleSet(namespaceID)
	if err != nil {
		return nil, err
	}

	mr, exists := crs.MappingRules[mappingRuleID]
	if !exists {
		return nil, s.mappingRuleNotFoundError(namespaceID, mappingRuleID)
	}
	return mr, nil
}

func (s *store) CreateMappingRule(
	namespaceID string,
	mrv *rules.MappingRuleView,
	uOpts r2.UpdateOptions,
) (*rules.MappingRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	newID, err := mutable.AddMappingRule(*mrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	return s.FetchMappingRule(namespaceID, newID)
}

func (s *store) UpdateMappingRule(
	namespaceID,
	mappingRuleID string,
	mrv *rules.MappingRuleView,
	uOpts r2.UpdateOptions,
) (*rules.MappingRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.UpdateMappingRule(*mrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	return s.FetchMappingRule(namespaceID, mappingRuleID)
}

func (s *store) DeleteMappingRule(
	namespaceID,
	mappingRuleID string,
	uOpts r2.UpdateOptions,
) error {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return s.handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.DeleteMappingRule(mappingRuleID, s.newUpdateMeta(uOpts))
	if err != nil {
		return s.handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return s.handleUpstreamError(err)
	}

	return nil
}

func (s *store) FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]*rules.MappingRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	mrs, err := rs.MappingRules()
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	hist, exists := mrs[mappingRuleID]
	if !exists {
		return nil, s.mappingRuleNotFoundError(namespaceID, mappingRuleID)
	}

	return hist, nil
}

func (s *store) FetchRollupRule(namespaceID string, mappingRuleID string) (*rules.RollupRuleView, error) {
	crs, err := s.FetchRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	rr, exists := crs.RollupRules[mappingRuleID]
	if !exists {
		return nil, s.rollupRuleNotFoundError(namespaceID, mappingRuleID)
	}
	return rr, nil
}

func (s *store) CreateRollupRule(
	namespaceID string,
	rrv *rules.RollupRuleView,
	uOpts r2.UpdateOptions,
) (*rules.RollupRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	newID, err := mutable.AddRollupRule(*rrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	return s.FetchRollupRule(namespaceID, newID)
}

func (s *store) UpdateRollupRule(
	namespaceID,
	rollupRuleID string,
	rrv *rules.RollupRuleView,
	uOpts r2.UpdateOptions,
) (*rules.RollupRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.UpdateRollupRule(*rrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	return s.FetchRollupRule(namespaceID, rollupRuleID)
}

func (s *store) DeleteRollupRule(
	namespaceID,
	rollupRuleID string,
	uOpts r2.UpdateOptions,
) error {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return s.handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.DeleteRollupRule(rollupRuleID, s.newUpdateMeta(uOpts))
	if err != nil {
		return s.handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return s.handleUpstreamError(err)
	}

	return nil
}

func (s *store) FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]*rules.RollupRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	rrs, err := rs.RollupRules()
	if err != nil {
		return nil, s.handleUpstreamError(err)
	}

	hist, exists := rrs[rollupRuleID]
	if !exists {
		return nil, s.rollupRuleNotFoundError(namespaceID, rollupRuleID)
	}
	return hist, nil
}

func (s *store) newUpdateMeta(uOpts r2.UpdateOptions) rules.UpdateMetadata {
	return s.updateHelper.NewUpdateMetadata(s.nowFn().UnixNano(), uOpts.Author())
}

func (s *store) rollupRuleNotFoundError(namespaceID, rollupRuleID string) error {
	return r2.NewNotFoundError(
		fmt.Sprintf("rollup rule: %s doesn't exist in Namespace: %s",
			rollupRuleID,
			namespaceID,
		),
	)
}

func (s *store) mappingRuleNotFoundError(namespaceID, mappingRuleID string) error {
	return r2.NewNotFoundError(
		fmt.Sprintf("mapping rule: %s doesn't exist in Namespace: %s",
			mappingRuleID,
			namespaceID,
		),
	)
}

func (s *store) handleUpstreamError(err error) error {
	if err == nil {
		return nil
	}

	switch err.(type) {
	case rules.RuleConflictError:
		return r2.NewConflictError(err.Error())
	case rules.ValidationError:
		return r2.NewBadInputError(err.Error())
	default:
		return r2.NewInternalError(err.Error())
	}
}
