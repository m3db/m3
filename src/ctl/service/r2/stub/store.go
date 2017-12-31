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
	"fmt"
	"time"

	"github.com/m3db/m3ctl/service/r2"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/instrument"
	"github.com/pborman/uuid"
)

type mappingRuleHistories map[string][]*rules.MappingRuleView
type rollupRuleHistories map[string][]*rules.RollupRuleView

type stubData struct {
	Namespaces        *rules.NamespacesView
	ErrorNamespace    string
	ConflictNamespace string
	RuleSets          map[string]*rules.RuleSetSnapshot
	MappingHistory    map[string]mappingRuleHistories
	RollupHistory     map[string]rollupRuleHistories
}

func makePolicy(s string) policy.Policy {
	p, _ := policy.ParsePolicy(s)
	return p
}

var (
	cutoverTimestamp = time.Now().UnixNano()
	dummyData        = stubData{
		ErrorNamespace:    "errNs",
		ConflictNamespace: "conflictNs",
		Namespaces: &rules.NamespacesView{
			Version: 1,
			Namespaces: []*rules.NamespaceView{
				&rules.NamespaceView{
					Name:              "ns1",
					ForRuleSetVersion: 1,
					Tombstoned:        false,
				},
				&rules.NamespaceView{
					Name:              "ns2",
					ForRuleSetVersion: 1,
					Tombstoned:        false,
				},
			},
		},
		RuleSets: map[string]*rules.RuleSetSnapshot{
			"ns1": &rules.RuleSetSnapshot{
				Namespace:    "ns1",
				Version:      1,
				CutoverNanos: cutoverTimestamp,
				MappingRules: map[string]*rules.MappingRuleView{
					"mr_id1": &rules.MappingRuleView{
						ID:           "mr_id1",
						Name:         "mr1",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1 tag2:val2",
						Policies: []policy.Policy{
							makePolicy("1m:10d"),
							makePolicy("10m:30d"),
						},
					},
					"mr_id2": &rules.MappingRuleView{
						ID:           "mr_id2",
						Name:         "mr2",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag2:val2",
						Policies: []policy.Policy{
							makePolicy("1m:10d"),
						},
					},
				},
				RollupRules: map[string]*rules.RollupRuleView{
					"rr_id1": &rules.RollupRuleView{
						ID:           "rr_id1",
						Name:         "rr1",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1 tag2:val2",
						Targets: []rules.RollupTargetView{
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|Min"),
								},
							},
						},
					},
					"rr_id2": &rules.RollupRuleView{
						ID:           "rr_id2",
						Name:         "rr2",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1",
						Targets: []rules.RollupTargetView{
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:30d|Min"),
								},
							},
						},
					},
				},
			},
			"ns2": &rules.RuleSetSnapshot{
				Namespace:    "ns2",
				Version:      1,
				CutoverNanos: cutoverTimestamp,
				MappingRules: map[string]*rules.MappingRuleView{},
				RollupRules: map[string]*rules.RollupRuleView{
					"rr_id3": &rules.RollupRuleView{
						ID:           "rr_id3",
						Name:         "rr1",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1 tag2:val2",
						Targets: []rules.RollupTargetView{
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|Min,Max"),
								},
							},
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|P999"),
								},
							},
						},
					},
				},
			},
		},
		MappingHistory: map[string]mappingRuleHistories{
			"ns1": mappingRuleHistories{
				"mr_id1": []*rules.MappingRuleView{
					&rules.MappingRuleView{
						ID:           "mr_id1",
						Name:         "mr1",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1 tag2:val2",
						Policies: []policy.Policy{
							makePolicy("1m:10d"),
							makePolicy("10m:30d"),
						},
					},
				},
				"mr_id2": []*rules.MappingRuleView{
					&rules.MappingRuleView{
						ID:           "mr_id2",
						Name:         "mr2",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1 tag2:val2",
						Policies: []policy.Policy{
							makePolicy("1m:10d"),
							makePolicy("10m:30d"),
						},
					},
				},
			},
			"ns2": nil,
		},
		RollupHistory: map[string]rollupRuleHistories{
			"ns1": rollupRuleHistories{
				"rr_id1": []*rules.RollupRuleView{
					&rules.RollupRuleView{
						ID:           "rr_id1",
						Name:         "rr1",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1 tag2:val2",
						Targets: []rules.RollupTargetView{
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|Min,Max"),
								},
							},
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|P999"),
								},
							},
						},
					},
					&rules.RollupRuleView{
						ID:           "rr_id1",
						Name:         "rr1",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1",
						Targets: []rules.RollupTargetView{
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|Min,Max"),
								},
							},
						},
					},
				},
			},
			"ns2": rollupRuleHistories{
				"rr_id3": []*rules.RollupRuleView{
					&rules.RollupRuleView{
						ID:           "rr_id1",
						Name:         "rr1",
						CutoverNanos: cutoverTimestamp,
						Filter:       "tag1:val1 tag2:val2",
						Targets: []rules.RollupTargetView{
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|Min,Max"),
								},
							},
							rules.RollupTargetView{
								Name: "testTarget",
								Tags: []string{"tag1", "tag2"},
								Policies: []policy.Policy{
									makePolicy("1m:10d|P999"),
								},
							},
						},
					},
				},
			},
		},
	}
)

// Operator contains the data necessary to implement stubbed out implementations for various r2 operations.
type store struct {
	data  *stubData
	iOpts instrument.Options
}

// NewStore creates a new stub
func NewStore(iOpts instrument.Options) r2.Store {
	return &store{data: &dummyData, iOpts: iOpts}
}

func (s *store) FetchNamespaces() (*rules.NamespacesView, error) {
	return s.data.Namespaces, nil
}

func (s *store) CreateNamespace(namespaceID string, uOpts r2.UpdateOptions) (*rules.NamespaceView, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError(fmt.Sprintf("could not create namespace: %s", namespaceID))
	case s.data.ConflictNamespace:
		return nil, r2.NewVersionError(fmt.Sprintf("namespaces version mismatch"))
	default:
		for _, n := range s.data.Namespaces.Namespaces {
			if namespaceID == n.Name {
				return nil, r2.NewConflictError(fmt.Sprintf("namespace %s already exists", namespaceID))
			}
		}

		newView := &rules.NamespaceView{
			Name:              namespaceID,
			ForRuleSetVersion: 1,
		}

		s.data.Namespaces.Namespaces = append(s.data.Namespaces.Namespaces, newView)
		s.data.RuleSets[namespaceID] = &rules.RuleSetSnapshot{
			Namespace:    namespaceID,
			Version:      1,
			CutoverNanos: time.Now().UnixNano(),
			MappingRules: make(map[string]*rules.MappingRuleView),
			RollupRules:  make(map[string]*rules.RollupRuleView),
		}
		return newView, nil
	}
}

func (s *store) DeleteNamespace(namespaceID string, uOpts r2.UpdateOptions) error {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return r2.NewInternalError("could not delete namespace")
	case s.data.ConflictNamespace:
		return r2.NewVersionError("namespace version mismatch")
	default:
		for i, n := range s.data.Namespaces.Namespaces {
			if namespaceID == n.Name {
				s.data.Namespaces.Namespaces = append(s.data.Namespaces.Namespaces[:i], s.data.Namespaces.Namespaces[i+1:]...)
				return nil
			}
		}
		return r2.NewConflictError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
	}
}

func (s *store) FetchRuleSet(namespaceID string) (*rules.RuleSetSnapshot, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError(fmt.Sprintf("could not fetch namespace: %s", namespaceID))
	default:
		for _, n := range s.data.Namespaces.Namespaces {
			if namespaceID == n.Name {
				rs := s.data.RuleSets[namespaceID]
				return rs, nil
			}
		}
		return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
	}
}

func (s *store) FetchMappingRule(namespaceID string, mappingRuleID string) (*rules.MappingRuleView, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError(fmt.Sprintf("could not fetch mappingRule: %s in namespace: %s", namespaceID, mappingRuleID))
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		for _, m := range rs.MappingRules {
			if mappingRuleID == m.ID {
				return m, nil
			}
		}
		return nil, r2.NewNotFoundError(fmt.Sprintf("mappingRule: %s doesn't exist in Namespace: %s", mappingRuleID, namespaceID))
	}
}

func (s *store) CreateMappingRule(
	namespaceID string,
	mrv *rules.MappingRuleView,
	uOpts r2.UpdateOptions,
) (*rules.MappingRuleView, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError("could not create mapping rule")
	case s.data.ConflictNamespace:
		return nil, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}

		for _, m := range rs.MappingRules {
			if mrv.Name == m.Name {
				return nil, r2.NewConflictError(fmt.Sprintf("mapping rule: %s already exists in namespace: %s", mrv.Name, namespaceID))
			}
		}
		newID := uuid.New()
		newRule := &rules.MappingRuleView{
			ID:           newID,
			Name:         mrv.Name,
			CutoverNanos: time.Now().UnixNano(),
			Filter:       mrv.Filter,
			Policies:     mrv.Policies,
		}
		rs.MappingRules[newID] = newRule
		return newRule, nil
	}
}

func (s *store) UpdateMappingRule(
	namespaceID,
	mappingRuleID string,
	mrv *rules.MappingRuleView,
	uOpts r2.UpdateOptions,
) (*rules.MappingRuleView, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError("could not update mapping rule.")
	case s.data.ConflictNamespace:
		return nil, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}

		for i, m := range rs.MappingRules {
			if mappingRuleID == m.ID {
				newRule := &rules.MappingRuleView{
					ID:           "new",
					Name:         mrv.Name,
					CutoverNanos: time.Now().UnixNano(),
					Filter:       mrv.Filter,
					Policies:     mrv.Policies,
				}
				rs.MappingRules[i] = newRule
				return newRule, nil
			}
		}
		return nil, r2.NewNotFoundError(fmt.Sprintf("mapping rule: %s doesn't exist in namespace: %s", mappingRuleID, namespaceID))
	}
}

func (s *store) DeleteMappingRule(
	namespaceID,
	mappingRuleID string,
	uOpts r2.UpdateOptions,
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
		_, exists = rs.MappingRules[mappingRuleID]
		if !exists {
			return r2.NewNotFoundError(fmt.Sprintf("mapping rule: %s doesn't exist in namespace: %s", mappingRuleID, namespaceID))
		}
		delete(rs.MappingRules, mappingRuleID)
		return nil
	}
}

func (s *store) FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]*rules.MappingRuleView, error) {
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

func (s *store) FetchRollupRule(namespaceID, rollupRuleID string) (*rules.RollupRuleView, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError(fmt.Sprintf("Could not fetch rollupRule: %s in namespace: %s", namespaceID, rollupRuleID))
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		for _, r := range rs.RollupRules {
			if rollupRuleID == r.ID {
				return r, nil
			}
		}
		return nil, r2.NewNotFoundError(fmt.Sprintf("rollupRule: %s doesn't exist in Namespace: %s", rollupRuleID, namespaceID))
	}
}

func (s *store) CreateRollupRule(
	namespaceID string,
	rrv *rules.RollupRuleView,
	uOpts r2.UpdateOptions,
) (*rules.RollupRuleView, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError("could not create rollup rule")
	case s.data.ConflictNamespace:
		return nil, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}
		for _, r := range rs.RollupRules {
			if rrv.Name == r.Name {
				return nil, r2.NewConflictError(fmt.Sprintf("rollup rule: %s already exists in namespace: %s", rrv.Name, namespaceID))
			}
		}
		newID := uuid.New()
		newRule := &rules.RollupRuleView{
			ID:           newID,
			Name:         rrv.Name,
			CutoverNanos: time.Now().UnixNano(),
			Filter:       rrv.Filter,
			Targets:      rrv.Targets,
		}
		rs.RollupRules[newID] = newRule
		return newRule, nil
	}
}

func (s *store) UpdateRollupRule(
	namespaceID,
	rollupRuleID string,
	rrv *rules.RollupRuleView,
	uOpts r2.UpdateOptions,
) (*rules.RollupRuleView, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError("could not update rollup rule.")
	case s.data.ConflictNamespace:
		return nil, r2.NewVersionError("namespaces version mismatch")
	default:
		rs, exists := s.data.RuleSets[namespaceID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
		}

		_, exists = rs.RollupRules[rollupRuleID]
		if !exists {
			return nil, r2.NewNotFoundError(fmt.Sprintf("rollup rule: %s doesn't exist in namespace: %s", rollupRuleID, namespaceID))
		}

		newRule := &rules.RollupRuleView{
			ID:           rollupRuleID,
			Name:         rrv.Name,
			CutoverNanos: time.Now().UnixNano(),
			Filter:       rrv.Filter,
			Targets:      rrv.Targets,
		}
		rs.RollupRules[rollupRuleID] = newRule
		return newRule, nil
	}
}

func (s *store) DeleteRollupRule(
	namespaceID,
	rollupRuleID string,
	uOpts r2.UpdateOptions,
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

		_, exists = rs.RollupRules[rollupRuleID]
		if !exists {
			return r2.NewNotFoundError(fmt.Sprintf("rollup rule: %s doesn't exist in namespace: %s", rollupRuleID, namespaceID))
		}
		delete(rs.RollupRules, rollupRuleID)
		return nil
	}
}

func (s *store) FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]*rules.RollupRuleView, error) {
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
