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

package stub

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/ctl/service/r2"
	r2store "github.com/m3db/m3/src/ctl/service/r2/store"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/rules/view/changes"
	"github.com/m3db/m3/src/metrics/x/bytes"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/pborman/uuid"
)

type mappingRuleHistories map[string][]view.MappingRule
type rollupRuleHistories map[string][]view.RollupRule

type stubData struct {
	Namespaces        view.Namespaces
	ErrorNamespace    string
	ConflictNamespace string
	RuleSets          map[string]view.RuleSet
	MappingHistory    map[string]mappingRuleHistories
	RollupHistory     map[string]rollupRuleHistories
}

var (
	errNotImplemented = errors.New("not implemented")
	cutoverMillis     = time.Now().UnixNano() / int64(time.Millisecond/time.Nanosecond)
)

// Operator contains the data necessary to implement stubbed out implementations for various r2 operations.
type store struct {
	data  *stubData
	iOpts instrument.Options
}

// NewStore creates a new stub
func NewStore(iOpts instrument.Options) (r2store.Store, error) {
	dummyData, err := buildDummyData()
	if err != nil {
		return nil, err
	}

	return &store{data: &dummyData, iOpts: iOpts}, err
}

func buildDummyData() (stubData, error) {
	rollup1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup3, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup4, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.P999),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup5, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup6, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.P999),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup7, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup8, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	if err != nil {
		return stubData{}, err
	}
	rollup9, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.P999),
	)
	if err != nil {
		return stubData{}, err
	}
	return stubData{
		ErrorNamespace:    "errNs",
		ConflictNamespace: "conflictNs",
		Namespaces: view.Namespaces{
			Version: 1,
			Namespaces: []view.Namespace{
				{
					ID:                "ns1",
					ForRuleSetVersion: 1,
					Tombstoned:        false,
				},
				{
					ID:                "ns2",
					ForRuleSetVersion: 1,
					Tombstoned:        false,
				},
			},
		},
		RuleSets: map[string]view.RuleSet{
			"ns1": {
				Namespace:     "ns1",
				Version:       1,
				CutoverMillis: cutoverMillis,
				MappingRules: []view.MappingRule{
					{
						ID:            "mr_id1",
						Name:          "mr1",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1 tag2:val2",
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("1m:10d"),
							policy.MustParseStoragePolicy("10m:30d"),
						},
					},
					{
						ID:            "mr_id2",
						Name:          "mr2",
						CutoverMillis: cutoverMillis,
						Filter:        "tag2:val2",
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("1m:10d"),
						},
					},
				},
				RollupRules: []view.RollupRule{
					{
						ID:            "rr_id1",
						Name:          "rr1",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1 tag2:val2",
						Targets: []view.RollupTarget{
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup1,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
						},
					},
					{
						ID:            "rr_id2",
						Name:          "rr2",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1",
						Targets: []view.RollupTarget{
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup2,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:30d"),
								},
							},
						},
					},
				},
			},
			"ns2": {
				Namespace:     "ns2",
				Version:       1,
				CutoverMillis: cutoverMillis,
				MappingRules:  []view.MappingRule{},
				RollupRules: []view.RollupRule{
					{
						ID:            "rr_id3",
						Name:          "rr1",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1 tag2:val2",
						Targets: []view.RollupTarget{
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup3,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup4,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
						},
					},
				},
			},
		},
		MappingHistory: map[string]mappingRuleHistories{
			"ns1": {
				"mr_id1": []view.MappingRule{
					{
						ID:            "mr_id1",
						Name:          "mr1",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1 tag2:val2",
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("1m:10d"),
							policy.MustParseStoragePolicy("10m:30d"),
						},
					},
				},
				"mr_id2": []view.MappingRule{
					{
						ID:            "mr_id2",
						Name:          "mr2",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1 tag2:val2",
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("1m:10d"),
							policy.MustParseStoragePolicy("10m:30d"),
						},
					},
				},
			},
			"ns2": nil,
		},
		RollupHistory: map[string]rollupRuleHistories{
			"ns1": {
				"rr_id1": []view.RollupRule{
					{
						ID:            "rr_id1",
						Name:          "rr1",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1 tag2:val2",
						Targets: []view.RollupTarget{
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup5,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup6,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
						},
					},
					{
						ID:            "rr_id1",
						Name:          "rr1",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1",
						Targets: []view.RollupTarget{
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup7,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
						},
					},
				},
			},
			"ns2": {
				"rr_id3": []view.RollupRule{
					{
						ID:            "rr_id1",
						Name:          "rr1",
						CutoverMillis: cutoverMillis,
						Filter:        "tag1:val1 tag2:val2",
						Targets: []view.RollupTarget{
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup8,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rollup9,
									},
								}),
								StoragePolicies: policy.StoragePolicies{
									policy.MustParseStoragePolicy("1m:10d"),
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (s *store) FetchNamespaces() (view.Namespaces, error) {
	return s.data.Namespaces, nil
}

func (s *store) CreateNamespace(
	namespaceID string,
	uOpts r2store.UpdateOptions,
) (view.Namespace, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.Namespace{}, r2.NewInternalError(fmt.Sprintf("could not create namespace: %s", namespaceID))
	case s.data.ConflictNamespace:
		return view.Namespace{}, r2.NewVersionError(fmt.Sprintf("namespaces version mismatch"))
	default:
		for _, n := range s.data.Namespaces.Namespaces {
			if namespaceID == n.ID {
				return view.Namespace{}, r2.NewConflictError(fmt.Sprintf("namespace %s already exists", namespaceID))
			}
		}

		newView := view.Namespace{
			ID:                namespaceID,
			ForRuleSetVersion: 1,
		}

		s.data.Namespaces.Namespaces = append(s.data.Namespaces.Namespaces, newView)
		s.data.RuleSets[namespaceID] = view.RuleSet{
			Namespace:     namespaceID,
			Version:       1,
			CutoverMillis: time.Now().UnixNano(),
		}
		return newView, nil
	}
}

func (s *store) ValidateRuleSet(rs view.RuleSet) error {
	// Assumes no validation config for stub store so all rule sets are valid.
	return nil
}

// This function is not supported. Use mocks package.
func (s *store) UpdateRuleSet(
	rsChanges changes.RuleSetChanges,
	version int,
	uOpts r2store.UpdateOptions,
) (view.RuleSet, error) {
	return view.RuleSet{}, errNotImplemented
}

func (s *store) DeleteNamespace(namespaceID string, uOpts r2store.UpdateOptions) error {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return r2.NewInternalError("could not delete namespace")
	case s.data.ConflictNamespace:
		return r2.NewVersionError("namespace version mismatch")
	default:
		for i, n := range s.data.Namespaces.Namespaces {
			if namespaceID == n.ID {
				s.data.Namespaces.Namespaces = append(s.data.Namespaces.Namespaces[:i], s.data.Namespaces.Namespaces[i+1:]...)
				return nil
			}
		}
		return r2.NewConflictError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
	}
}

func (s *store) FetchRuleSetSnapshot(namespaceID string) (view.RuleSet, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.RuleSet{}, r2.NewInternalError(fmt.Sprintf("could not fetch namespace: %s", namespaceID))
	default:
		for _, n := range s.data.Namespaces.Namespaces {
			if namespaceID == n.ID {
				rs := s.data.RuleSets[namespaceID]
				return rs, nil
			}
		}
		return view.RuleSet{}, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
	}
}

func (s *store) FetchMappingRule(namespaceID string, mappingRuleID string) (view.MappingRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.MappingRule{}, r2.NewInternalError(fmt.Sprintf("could not fetch mappingRule: %s in namespace: %s", namespaceID, mappingRuleID))
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return view.MappingRule{}, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		for _, m := range rs.MappingRules {
			if mappingRuleID == m.ID {
				return m, nil
			}
		}
		return view.MappingRule{}, r2.NewNotFoundError(fmt.Sprintf("mappingRule: %s doesn't exist in Namespace: %s", mappingRuleID, namespaceID))
	}
}

func (s *store) CreateMappingRule(
	namespaceID string,
	mrv view.MappingRule,
	uOpts r2store.UpdateOptions,
) (view.MappingRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.MappingRule{}, r2.NewInternalError("could not create mapping rule")
	case s.data.ConflictNamespace:
		return view.MappingRule{}, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return view.MappingRule{}, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}

		for _, m := range rs.MappingRules {
			if mrv.Name == m.Name {
				return view.MappingRule{}, r2.NewConflictError(fmt.Sprintf("mapping rule: %s already exists in namespace: %s", mrv.Name, namespaceID))
			}
		}
		newID := uuid.New()
		newRule := view.MappingRule{
			ID:              newID,
			Name:            mrv.Name,
			CutoverMillis:   time.Now().UnixNano(),
			Filter:          mrv.Filter,
			AggregationID:   mrv.AggregationID,
			StoragePolicies: mrv.StoragePolicies,
		}
		rs.MappingRules = append(rs.MappingRules, newRule)
		return newRule, nil
	}
}

func (s *store) UpdateMappingRule(
	namespaceID,
	mappingRuleID string,
	mrv view.MappingRule,
	uOpts r2store.UpdateOptions,
) (view.MappingRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.MappingRule{}, r2.NewInternalError("could not update mapping rule.")
	case s.data.ConflictNamespace:
		return view.MappingRule{}, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return view.MappingRule{}, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}

		for i, m := range rs.MappingRules {
			if mappingRuleID == m.ID {
				newRule := view.MappingRule{
					ID:              "new",
					Name:            mrv.Name,
					CutoverMillis:   time.Now().UnixNano(),
					Filter:          mrv.Filter,
					AggregationID:   mrv.AggregationID,
					StoragePolicies: mrv.StoragePolicies,
				}
				rs.MappingRules[i] = newRule
				return newRule, nil
			}
		}
		return view.MappingRule{}, r2.NewNotFoundError(fmt.Sprintf("mapping rule: %s doesn't exist in namespace: %s", mappingRuleID, namespaceID))
	}
}

func (s *store) DeleteMappingRule(
	namespaceID,
	mappingRuleID string,
	uOpts r2store.UpdateOptions,
) error {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return r2.NewInternalError("could not delete mapping rule.")
	case s.data.ConflictNamespace:
		return r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		foundIdx := -1
		for i, rule := range rs.MappingRules {
			if rule.ID == mappingRuleID {
				foundIdx = i
				break
			}
		}
		if foundIdx == -1 {
			return r2.NewNotFoundError(fmt.Sprintf("mapping rule: %s doesn't exist in namespace: %s", mappingRuleID, namespaceID))
		}
		rs.MappingRules = append(rs.MappingRules[:foundIdx], rs.MappingRules[foundIdx+1:]...)
		return nil
	}
}

func (s *store) FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]view.MappingRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError(fmt.Sprintf("Could not fetch mappingRuleID: %s in namespace: %s", namespaceID, mappingRuleID))
	default:
		ns, exists := s.data.MappingHistory[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		hist, exists := ns[mappingRuleID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("mappingRule: %s doesn't exist in Namespace: %s", mappingRuleID, namespaceID))
		}
		return hist, nil
	}
}

func (s *store) FetchRollupRule(namespaceID, rollupRuleID string) (view.RollupRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.RollupRule{}, r2.NewInternalError(fmt.Sprintf("Could not fetch rollupRule: %s in namespace: %s", namespaceID, rollupRuleID))
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return view.RollupRule{}, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		for _, r := range rs.RollupRules {
			if rollupRuleID == r.ID {
				return r, nil
			}
		}
		return view.RollupRule{}, r2.NewNotFoundError(fmt.Sprintf("rollupRule: %s doesn't exist in Namespace: %s", rollupRuleID, namespaceID))
	}
}

func (s *store) CreateRollupRule(
	namespaceID string,
	rrv view.RollupRule,
	uOpts r2store.UpdateOptions,
) (view.RollupRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.RollupRule{}, r2.NewInternalError("could not create rollup rule")
	case s.data.ConflictNamespace:
		return view.RollupRule{}, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return view.RollupRule{}, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		for _, r := range rs.RollupRules {
			if rrv.Name == r.Name {
				return view.RollupRule{}, r2.NewConflictError(fmt.Sprintf("rollup rule: %s already exists in namespace: %s", rrv.Name, namespaceID))
			}
		}
		newID := uuid.New()
		newRule := view.RollupRule{
			ID:            newID,
			Name:          rrv.Name,
			CutoverMillis: time.Now().UnixNano(),
			Filter:        rrv.Filter,
			Targets:       rrv.Targets,
		}
		rs.RollupRules = append(rs.RollupRules, newRule)
		return newRule, nil
	}
}

func (s *store) UpdateRollupRule(
	namespaceID,
	rollupRuleID string,
	rrv view.RollupRule,
	uOpts r2store.UpdateOptions,
) (view.RollupRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return view.RollupRule{}, r2.NewInternalError("could not update rollup rule.")
	case s.data.ConflictNamespace:
		return view.RollupRule{}, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return view.RollupRule{}, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}

		for i, m := range rs.RollupRules {
			if rollupRuleID == m.ID {
				newRule := view.RollupRule{
					ID:            rollupRuleID,
					Name:          rrv.Name,
					CutoverMillis: time.Now().UnixNano(),
					Filter:        rrv.Filter,
					Targets:       rrv.Targets,
				}
				rs.RollupRules[i] = newRule
				return newRule, nil
			}
		}
		return view.RollupRule{}, r2.NewNotFoundError(fmt.Sprintf("rollup rule: %s doesn't exist in namespace: %s", rollupRuleID, namespaceID))
	}
}

func (s *store) DeleteRollupRule(
	namespaceID,
	rollupRuleID string,
	uOpts r2store.UpdateOptions,
) error {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return r2.NewInternalError("could not delete rollup rule.")
	case s.data.ConflictNamespace:
		return r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}

		foundIdx := -1
		for i, rule := range rs.RollupRules {
			if rule.ID == rollupRuleID {
				foundIdx = i
				break
			}
		}
		if foundIdx == -1 {
			return r2.NewNotFoundError(fmt.Sprintf("rollup rule: %s doesn't exist in namespace: %s", rollupRuleID, namespaceID))
		}
		rs.RollupRules = append(rs.RollupRules[:foundIdx], rs.RollupRules[foundIdx+1:]...)
		return nil
	}
}

func (s *store) FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]view.RollupRule, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError(fmt.Sprintf("Could not fetch rollupRule: %s in namespace: %s", namespaceID, rollupRuleID))
	default:
		ns, exists := s.data.RollupHistory[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		hist, exists := ns[rollupRuleID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("rollupRule: %s doesn't exist in Namespace: %s", rollupRuleID, namespaceID))
		}
		return hist, nil
	}
}

func (s *store) Close() {}

// nolint: unparam
func b(str string) []byte        { return []byte(str) }
func bs(strs ...string) [][]byte { return bytes.ArraysFromStringArray(strs) }
