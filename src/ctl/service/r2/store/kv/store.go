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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/ctl/service/r2"
	r2store "github.com/m3db/m3/src/ctl/service/r2/store"
	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/rules/view/changes"
	"github.com/m3db/m3/src/x/clock"
	xerrors "github.com/m3db/m3/src/x/errors"
)

type store struct {
	nowFn        clock.NowFn
	opts         StoreOptions
	ruleStore    rules.Store
	updateHelper rules.RuleSetUpdateHelper
}

var errNilValidator = errors.New("no validator set on StoreOptions so validation is not applicable")

// NewStore returns a new service that knows how to talk to a kv backed r2 store.
func NewStore(rs rules.Store, opts StoreOptions) r2store.Store {
	clockOpts := opts.ClockOptions()
	updateHelper := rules.NewRuleSetUpdateHelper(opts.RuleUpdatePropagationDelay())
	return &store{
		nowFn:        clockOpts.NowFn(),
		opts:         opts,
		ruleStore:    rs,
		updateHelper: updateHelper,
	}
}

func (s *store) FetchNamespaces() (view.Namespaces, error) {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return view.Namespaces{}, handleUpstreamError(err)
	}

	namespaces, err := nss.NamespacesView()
	if err != nil {
		return view.Namespaces{}, handleUpstreamError(err)
	}

	liveNss := make([]view.Namespace, 0, len(namespaces.Namespaces))
	for _, ns := range namespaces.Namespaces {
		if !ns.Tombstoned {
			liveNss = append(liveNss, ns)
		}
	}

	return view.Namespaces{
		Version:    namespaces.Version,
		Namespaces: liveNss,
	}, nil
}

func (s *store) ValidateRuleSet(rs view.RuleSet) error {
	validator := s.opts.Validator()
	// If no validator is set, then the validation functionality is not applicable.
	if validator == nil {
		return errNilValidator
	}

	return handleUpstreamError(validator.ValidateSnapshot(rs))
}

func (s *store) UpdateRuleSet(
	rsChanges changes.RuleSetChanges,
	version int,
	uOpts r2store.UpdateOptions,
) (view.RuleSet, error) {
	rs, err := s.ruleStore.ReadRuleSet(rsChanges.Namespace)
	if err != nil {
		return view.RuleSet{}, handleUpstreamError(err)
	}
	// If ruleset version fetched from KV matches the change set version,
	// the check and set operation will succeed if the underlying
	// KV version remains the same before the change is committed and fail otherwise.
	// If the fetched version doesn't match, we will fail fast.
	if version != rs.Version() {
		return view.RuleSet{}, r2.NewConflictError(fmt.Sprintf(
			"ruleset version mismatch: current version=%d, expected version=%d",
			rs.Version(),
			version,
		))
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.ApplyRuleSetChanges(rsChanges, s.newUpdateMeta(uOpts))
	if err != nil {
		return view.RuleSet{}, handleUpstreamError(err)
	}
	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return view.RuleSet{}, handleUpstreamError(err)
	}

	return s.FetchRuleSetSnapshot(rsChanges.Namespace)
}

func (s *store) CreateNamespace(
	namespaceID string,
	uOpts r2store.UpdateOptions,
) (view.Namespace, error) {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return view.Namespace{}, handleUpstreamError(err)
	}

	meta := s.newUpdateMeta(uOpts)
	revived, err := nss.AddNamespace(namespaceID, meta)
	if err != nil {
		return view.Namespace{}, handleUpstreamError(err)
	}

	rs := rules.NewEmptyRuleSet(namespaceID, meta)
	if revived {
		rawRs, err := s.ruleStore.ReadRuleSet(namespaceID)
		if err != nil {
			return view.Namespace{}, handleUpstreamError(err)
		}
		rs = rawRs.ToMutableRuleSet().Clone()
		if err = rs.Revive(meta); err != nil {
			return view.Namespace{}, handleUpstreamError(err)
		}
	}

	if err = s.ruleStore.WriteAll(nss, rs); err != nil {
		return view.Namespace{}, handleUpstreamError(err)
	}

	nss, err = s.ruleStore.ReadNamespaces()
	if err != nil {
		return view.Namespace{}, handleUpstreamError(err)
	}

	ns, err := nss.Namespace(namespaceID)
	if err != nil {
		return view.Namespace{}, r2.NewNotFoundError(fmt.Sprintf("namespace: %s does not exist", namespaceID))
	}

	// Get the latest view of the namespace.
	nsView, err := ns.NamespaceView(len(ns.Snapshots()) - 1)
	if err != nil {
		return view.Namespace{}, handleUpstreamError(err)
	}

	return nsView, nil
}

func (s *store) DeleteNamespace(namespaceID string, uOpts r2store.UpdateOptions) error {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return handleUpstreamError(err)
	}

	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return handleUpstreamError(err)
	}

	meta := s.newUpdateMeta(uOpts)
	err = nss.DeleteNamespace(namespaceID, rs.Version(), meta)
	if err != nil {
		return handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.Delete(meta)
	if err != nil {
		return handleUpstreamError(err)
	}

	if err = s.ruleStore.WriteAll(nss, mutable); err != nil {
		return handleUpstreamError(err)
	}

	return nil
}

func (s *store) FetchRuleSetSnapshot(namespaceID string) (view.RuleSet, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return view.RuleSet{}, handleUpstreamError(err)
	}
	return rs.Latest()
}

func (s *store) FetchMappingRule(
	namespaceID string,
	mappingRuleID string,
) (view.MappingRule, error) {
	ruleset, err := s.FetchRuleSetSnapshot(namespaceID)
	if err != nil {
		return view.MappingRule{}, err
	}

	for _, mr := range ruleset.MappingRules {
		if mr.ID == mappingRuleID {
			return mr, nil
		}
	}

	return view.MappingRule{}, mappingRuleNotFoundError(namespaceID, mappingRuleID)
}

func (s *store) CreateMappingRule(
	namespaceID string,
	mrv view.MappingRule,
	uOpts r2store.UpdateOptions,
) (view.MappingRule, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return view.MappingRule{}, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	newID, err := mutable.AddMappingRule(mrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return view.MappingRule{}, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return view.MappingRule{}, handleUpstreamError(err)
	}

	return s.FetchMappingRule(namespaceID, newID)
}

func (s *store) UpdateMappingRule(
	namespaceID string,
	mappingRuleID string,
	mrv view.MappingRule,
	uOpts r2store.UpdateOptions,
) (view.MappingRule, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return view.MappingRule{}, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.UpdateMappingRule(mrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return view.MappingRule{}, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return view.MappingRule{}, handleUpstreamError(err)
	}

	return s.FetchMappingRule(namespaceID, mappingRuleID)
}

func (s *store) DeleteMappingRule(
	namespaceID string,
	mappingRuleID string,
	uOpts r2store.UpdateOptions,
) error {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.DeleteMappingRule(mappingRuleID, s.newUpdateMeta(uOpts))
	if err != nil {
		return handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return handleUpstreamError(err)
	}

	return nil
}

func (s *store) FetchMappingRuleHistory(
	namespaceID string,
	mappingRuleID string,
) ([]view.MappingRule, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	mrs, err := rs.MappingRules()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	for _, mappings := range mrs {
		if len(mappings) > 0 && mappings[0].ID == mappingRuleID {
			return mappings, nil
		}
	}

	return nil, mappingRuleNotFoundError(namespaceID, mappingRuleID)
}

func (s *store) FetchRollupRule(
	namespaceID string,
	rollupRuleID string,
) (view.RollupRule, error) {
	ruleset, err := s.FetchRuleSetSnapshot(namespaceID)
	if err != nil {
		return view.RollupRule{}, handleUpstreamError(err)
	}

	for _, rr := range ruleset.RollupRules {
		if rr.ID == rollupRuleID {
			return rr, nil
		}
	}

	return view.RollupRule{}, rollupRuleNotFoundError(namespaceID, rollupRuleID)
}

func (s *store) CreateRollupRule(
	namespaceID string,
	rrv view.RollupRule,
	uOpts r2store.UpdateOptions,
) (view.RollupRule, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return view.RollupRule{}, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	newID, err := mutable.AddRollupRule(rrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return view.RollupRule{}, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return view.RollupRule{}, handleUpstreamError(err)
	}

	return s.FetchRollupRule(namespaceID, newID)
}

func (s *store) UpdateRollupRule(
	namespaceID,
	rollupRuleID string,
	rrv view.RollupRule,
	uOpts r2store.UpdateOptions,
) (view.RollupRule, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return view.RollupRule{}, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.UpdateRollupRule(rrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return view.RollupRule{}, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return view.RollupRule{}, handleUpstreamError(err)
	}

	return s.FetchRollupRule(namespaceID, rollupRuleID)
}

func (s *store) DeleteRollupRule(
	namespaceID string,
	rollupRuleID string,
	uOpts r2store.UpdateOptions,
) error {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.DeleteRollupRule(rollupRuleID, s.newUpdateMeta(uOpts))
	if err != nil {
		return handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return handleUpstreamError(err)
	}

	return nil
}

func (s *store) FetchRollupRuleHistory(
	namespaceID string,
	rollupRuleID string,
) ([]view.RollupRule, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	rrs, err := rs.RollupRules()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	for _, rollups := range rrs {
		if len(rollups) > 0 && rollups[0].ID == rollupRuleID {
			return rollups, nil
		}
	}

	return nil, rollupRuleNotFoundError(namespaceID, rollupRuleID)
}

func (s *store) Close() { s.ruleStore.Close() }

func (s *store) newUpdateMeta(uOpts r2store.UpdateOptions) rules.UpdateMetadata {
	return s.updateHelper.NewUpdateMetadata(s.nowFn().UnixNano(), uOpts.Author())
}

func mappingRuleNotFoundError(namespaceID, mappingRuleID string) error {
	return r2.NewNotFoundError(
		fmt.Sprintf("mapping rule: %s doesn't exist in Namespace: %s",
			mappingRuleID,
			namespaceID,
		),
	)
}

func rollupRuleNotFoundError(namespaceID, rollupRuleID string) error {
	return r2.NewNotFoundError(
		fmt.Sprintf("rollup rule: %s doesn't exist in Namespace: %s",
			rollupRuleID,
			namespaceID,
		),
	)
}

func handleUpstreamError(err error) error {
	if err == nil {
		return nil
	}

	// If this is a contained error, extracts the inner error.
	if e := xerrors.InnerError(err); e != nil {
		err = e
	}

	switch err.(type) {
	case merrors.InvalidInputError, merrors.StaleDataError:
		return r2.NewConflictError(err.Error())
	case merrors.ValidationError:
		return r2.NewBadInputError(err.Error())
	case merrors.NotFoundError:
		return r2.NewNotFoundError(err.Error())
	default:
		return r2.NewInternalError(err.Error())
	}
}
