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

	"github.com/m3db/m3ctl/service/r2"
	r2store "github.com/m3db/m3ctl/service/r2/store"
	merrors "github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3metrics/rules/models"
	"github.com/m3db/m3metrics/rules/models/changes"
	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
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

func (s *store) FetchNamespaces() (*models.NamespacesView, error) {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	namespaces, err := nss.NamespacesView()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	liveNss := make([]*models.NamespaceView, 0, len(namespaces.Namespaces))
	for _, ns := range namespaces.Namespaces {
		if !ns.Tombstoned {
			liveNss = append(liveNss, ns)
		}
	}

	return &models.NamespacesView{
		Namespaces: liveNss,
		Version:    namespaces.Version,
	}, nil
}

func (s *store) ValidateRuleSet(rs *models.RuleSetSnapshotView) error {
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
) (*models.RuleSetSnapshotView, error) {
	rs, err := s.ruleStore.ReadRuleSet(rsChanges.Namespace)
	if err != nil {
		return nil, handleUpstreamError(err)
	}
	// If ruleset version fetched from KV matches the change set version,
	// the check and set operation will succeed if the underlying
	// KV version remains the same before the change is committed and fail otherwise.
	// If the fetched version doesn't match, we will fail fast.
	if version != rs.Version() {
		return nil, r2.NewConflictError(fmt.Sprintf(
			"ruleset version mismatch: current version=%d, expected version=%d",
			rs.Version(),
			version,
		))
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.ApplyRuleSetChanges(rsChanges, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, handleUpstreamError(err)
	}
	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	return s.FetchRuleSetSnapshot(rsChanges.Namespace)
}
func (s *store) CreateNamespace(namespaceID string, uOpts r2store.UpdateOptions) (*models.NamespaceView, error) {
	nss, err := s.ruleStore.ReadNamespaces()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	meta := s.newUpdateMeta(uOpts)
	revived, err := nss.AddNamespace(namespaceID, meta)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	rs := rules.NewEmptyRuleSet(namespaceID, meta)
	if revived {
		rawRs, err := s.ruleStore.ReadRuleSet(namespaceID)
		if err != nil {
			return nil, handleUpstreamError(err)
		}
		rs = rawRs.ToMutableRuleSet().Clone()
		if err = rs.Revive(meta); err != nil {
			return nil, handleUpstreamError(err)
		}
	}

	if err = s.ruleStore.WriteAll(nss, rs); err != nil {
		return nil, handleUpstreamError(err)
	}

	nss, err = s.ruleStore.ReadNamespaces()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	ns, err := nss.Namespace(namespaceID)
	if err != nil {
		return nil, r2.NewNotFoundError(fmt.Sprintf("namespace: %s does not exist", namespaceID))
	}

	// Get the latest view of the namespace.
	view, err := ns.NamespaceView(len(ns.Snapshots()) - 1)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	return view, nil
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

func (s *store) FetchRuleSetSnapshot(namespaceID string) (*models.RuleSetSnapshotView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}
	return rs.Latest()
}

func (s *store) FetchMappingRule(namespaceID string, mappingRuleID string) (*models.MappingRuleView, error) {
	crs, err := s.FetchRuleSetSnapshot(namespaceID)
	if err != nil {
		return nil, err
	}

	mr, exists := crs.MappingRules[mappingRuleID]
	if !exists {
		return nil, mappingRuleNotFoundError(namespaceID, mappingRuleID)
	}
	return mr, nil
}

func (s *store) CreateMappingRule(
	namespaceID string,
	mrv *models.MappingRuleView,
	uOpts r2store.UpdateOptions,
) (*models.MappingRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	newID, err := mutable.AddMappingRule(*mrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	return s.FetchMappingRule(namespaceID, newID)
}

func (s *store) UpdateMappingRule(
	namespaceID,
	mappingRuleID string,
	mrv *models.MappingRuleView,
	uOpts r2store.UpdateOptions,
) (*models.MappingRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.UpdateMappingRule(*mrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	return s.FetchMappingRule(namespaceID, mappingRuleID)
}

func (s *store) DeleteMappingRule(
	namespaceID,
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

func (s *store) FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]*models.MappingRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	mrs, err := rs.MappingRules()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	hist, exists := mrs[mappingRuleID]
	if !exists {
		return nil, mappingRuleNotFoundError(namespaceID, mappingRuleID)
	}

	return hist, nil
}

func (s *store) FetchRollupRule(namespaceID string, mappingRuleID string) (*models.RollupRuleView, error) {
	crs, err := s.FetchRuleSetSnapshot(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	rr, exists := crs.RollupRules[mappingRuleID]
	if !exists {
		return nil, rollupRuleNotFoundError(namespaceID, mappingRuleID)
	}
	return rr, nil
}

func (s *store) CreateRollupRule(
	namespaceID string,
	rrv *models.RollupRuleView,
	uOpts r2store.UpdateOptions,
) (*models.RollupRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	newID, err := mutable.AddRollupRule(*rrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	return s.FetchRollupRule(namespaceID, newID)
}

func (s *store) UpdateRollupRule(
	namespaceID,
	rollupRuleID string,
	rrv *models.RollupRuleView,
	uOpts r2store.UpdateOptions,
) (*models.RollupRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	mutable := rs.ToMutableRuleSet().Clone()
	err = mutable.UpdateRollupRule(*rrv, s.newUpdateMeta(uOpts))
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	err = s.ruleStore.WriteRuleSet(mutable)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	return s.FetchRollupRule(namespaceID, rollupRuleID)
}

func (s *store) DeleteRollupRule(
	namespaceID,
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

func (s *store) FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]*models.RollupRuleView, error) {
	rs, err := s.ruleStore.ReadRuleSet(namespaceID)
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	rrs, err := rs.RollupRules()
	if err != nil {
		return nil, handleUpstreamError(err)
	}

	hist, exists := rrs[rollupRuleID]
	if !exists {
		return nil, rollupRuleNotFoundError(namespaceID, rollupRuleID)
	}
	return hist, nil
}

func (s *store) Close() { s.ruleStore.Close() }

func (s *store) newUpdateMeta(uOpts r2store.UpdateOptions) rules.UpdateMetadata {
	return s.updateHelper.NewUpdateMetadata(s.nowFn().UnixNano(), uOpts.Author())
}

func rollupRuleNotFoundError(namespaceID, rollupRuleID string) error {
	return r2.NewNotFoundError(
		fmt.Sprintf("rollup rule: %s doesn't exist in Namespace: %s",
			rollupRuleID,
			namespaceID,
		),
	)
}

func mappingRuleNotFoundError(namespaceID, mappingRuleID string) error {
	return r2.NewNotFoundError(
		fmt.Sprintf("mapping rule: %s doesn't exist in Namespace: %s",
			mappingRuleID,
			namespaceID,
		),
	)
}

func handleUpstreamError(err error) error {
	if err == nil {
		return nil
	}

	// If this is a contained error, extracts the inner error.
	if containedErr, ok := err.(xerrors.ContainedError); ok {
		err = containedErr.InnerError()
	}

	switch err.(type) {
	case merrors.InvalidInputError, merrors.StaleDataError:
		return r2.NewConflictError(err.Error())
	case merrors.ValidationError:
		return r2.NewBadInputError(err.Error())
	default:
		return r2.NewInternalError(err.Error())
	}
}
