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

	"github.com/m3db/m3ctl/r2"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/instrument"
	"github.com/pborman/uuid"
)

type mappingRuleHistories map[string][]*rules.MappingRuleView
type rollupRuleHistories map[string][]*rules.RollupRuleView

type stubData struct {
	Namespaces        []string
	ErrorNamespace    string
	ConflictNamespace string
	RuleSets          map[string]*r2.CurrentRuleSet
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
		Namespaces:        []string{"ns1", "ns2"},
		RuleSets: map[string]*r2.CurrentRuleSet{
			"ns1": &r2.CurrentRuleSet{
				Namespace:    "ns1",
				Version:      1,
				CutoverNanos: cutoverTimestamp,
				MappingRules: []*rules.MappingRuleView{
					&rules.MappingRuleView{
						ID:           "mr_id1",
						Name:         "mr1",
						CutoverNanos: cutoverTimestamp,
						Filters: map[string]string{
							"tag1": "val1",
							"tag2": "val2",
						},
						Policies: []policy.Policy{
							makePolicy("1m:10d"),
							makePolicy("10m:30d"),
						},
					},
					&rules.MappingRuleView{
						ID:           "mr_id2",
						Name:         "mr2",
						CutoverNanos: cutoverTimestamp,
						Filters: map[string]string{
							"tag2": "val2",
						},
						Policies: []policy.Policy{
							makePolicy("1m:10d"),
						},
					},
				},
				RollupRules: []*rules.RollupRuleView{
					&rules.RollupRuleView{
						ID:           "rr_id1",
						Name:         "rr1",
						CutoverNanos: cutoverTimestamp,
						Filters: map[string]string{
							"tag1": "val1",
							"tag2": "val2",
						},
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
					&rules.RollupRuleView{
						ID:           "rr_id2",
						Name:         "rr2",
						CutoverNanos: cutoverTimestamp,
						Filters: map[string]string{
							"tag1": "val1",
						},
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
			"ns2": &r2.CurrentRuleSet{
				Namespace:    "ns2",
				Version:      1,
				CutoverNanos: cutoverTimestamp,
				MappingRules: []*rules.MappingRuleView{},
				RollupRules: []*rules.RollupRuleView{
					&rules.RollupRuleView{
						ID:           "rr_id3",
						Name:         "rr1",
						CutoverNanos: cutoverTimestamp,
						Filters: map[string]string{
							"tag1": "val1",
							"tag2": "val2",
						},
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
						Filters: map[string]string{
							"tag1": "val1",
							"tag2": "val2",
						},
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
						Filters: map[string]string{
							"tag1": "val1",
							"tag2": "val2",
						},
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
						Filters: map[string]string{
							"tag1": "val1",
							"tag2": "val2",
						},
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
						Filters: map[string]string{
							"tag1": "val1",
						},
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
						Filters: map[string]string{
							"tag1": "val1",
							"tag2": "val2",
						},
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

func (s *store) FetchNamespaces() ([]string, int, error) {
	return s.data.Namespaces, 1, nil
}

func (s *store) CreateNamespace(namespaceID string) (string, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return "", r2.NewInternalError(fmt.Sprintf("could not create namespace: %s", namespaceID))
	case s.data.ConflictNamespace:
		return "", r2.NewVersionError(fmt.Sprintf("namespaces version mismatch"))
	default:
		for _, n := range s.data.Namespaces {
			if namespaceID == n {
				return "", r2.NewConflictError(fmt.Sprintf("namespace %s already exists", namespaceID))
			}
		}
		s.data.Namespaces = append(s.data.Namespaces, namespaceID)
		s.data.RuleSets[namespaceID] = &r2.CurrentRuleSet{
			Namespace:    namespaceID,
			Version:      1,
			CutoverNanos: time.Now().UnixNano(),
			MappingRules: make([]*rules.MappingRuleView, 0),
			RollupRules:  make([]*rules.RollupRuleView, 0),
		}
		return namespaceID, nil
	}
}

func (s *store) DeleteNamespace(namespaceID string) error {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return r2.NewInternalError("could not delete namespace")
	case s.data.ConflictNamespace:
		return r2.NewVersionError("namespace version mismatch")
	default:
		for i, n := range s.data.Namespaces {
			if namespaceID == n {
				s.data.Namespaces = append(s.data.Namespaces[:i], s.data.Namespaces[i+1:]...)
				return nil
			}
		}
		return r2.NewConflictError(fmt.Sprintf("namespace %s doesn't exist", namespaceID))
	}
}

func (s *store) FetchRuleSet(namespaceID string) (*r2.CurrentRuleSet, error) {
	switch namespaceID {
	case s.data.ErrorNamespace:
		return nil, r2.NewInternalError(fmt.Sprintf("could not fetch namespace: %s", namespaceID))
	default:
		for _, n := range s.data.Namespaces {
			if namespaceID == n {
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

func (s *store) CreateMappingRule(namespaceID string, mrv *rules.MappingRuleView) (*rules.MappingRuleView, error) {
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

		newRule := &rules.MappingRuleView{
			ID:           "new",
			Name:         mrv.Name,
			CutoverNanos: time.Now().UnixNano(),
			Filters:      mrv.Filters,
			Policies:     mrv.Policies,
		}
		rs.MappingRules = append(rs.MappingRules, newRule)
		return newRule, nil
	}
}

func (s *store) UpdateMappingRule(namespaceID, mappingRuleID string, mrv *rules.MappingRuleView) (*rules.MappingRuleView, error) {
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
					Filters:      mrv.Filters,
					Policies:     mrv.Policies,
				}
				rs.MappingRules[i] = newRule
				return newRule, nil
			}
		}
		return nil, r2.NewNotFoundError(fmt.Sprintf("mapping rule: %s doesn't exist in namespace: %s", mappingRuleID, namespaceID))
	}
}

func (s *store) DeleteMappingRule(namespaceID, mappingRuleID string) error {
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
		for i, m := range rs.MappingRules {
			if mappingRuleID == m.ID {
				rs.MappingRules = append(rs.MappingRules[:i], rs.MappingRules[i+1:]...)
				return nil
			}
		}
		return r2.NewNotFoundError(fmt.Sprintf("mapping rule: %s doesn't exist in namespace: %s", mappingRuleID, namespaceID))
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

func (s *store) CreateRollupRule(namespaceID string, rrv *rules.RollupRuleView) (*rules.RollupRuleView, error) {
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
		newRule := &rules.RollupRuleView{
			ID:           uuid.New(),
			Name:         rrv.Name,
			CutoverNanos: time.Now().UnixNano(),
			Filters:      rrv.Filters,
			Targets:      rrv.Targets,
		}
		rs.RollupRules = append(rs.RollupRules, newRule)
		return newRule, nil
	}
}

func (s *store) UpdateRollupRule(namespaceID, rollupRuleID string, rrv *rules.RollupRuleView) (*rules.RollupRuleView, error) {
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
		for i, r := range rs.RollupRules {
			if rollupRuleID == r.ID {
				newRule := &rules.RollupRuleView{
					ID:           r.ID,
					Name:         rrv.Name,
					CutoverNanos: time.Now().UnixNano(),
					Filters:      rrv.Filters,
					Targets:      rrv.Targets,
				}
				rs.RollupRules[i] = newRule
				return newRule, nil
			}
		}
		return nil, r2.NewNotFoundError(fmt.Sprintf("rollup rule: %s doesn't exist in namespace: %s", rollupRuleID, namespaceID))
	}
}

func (s *store) DeleteRollupRule(namespaceID, rollupRuleID string) error {
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
		for i, r := range rs.RollupRules {
			if rollupRuleID == r.ID {
				rs.RollupRules = append(rs.RollupRules[:i], rs.RollupRules[i+1:]...)
				return nil
			}
		}
		return r2.NewNotFoundError(fmt.Sprintf("rollup rule: %s doesn't exist in namespace: %s", rollupRuleID, namespaceID))
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
