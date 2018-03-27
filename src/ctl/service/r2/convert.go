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

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"

	"github.com/pborman/uuid"
)

type namespaceJSON struct {
	ID                string `json:"id" validate:"required"`
	ForRuleSetVersion int    `json:"forRuleSetVersion"`
}

func newNamespaceJSON(nv *models.NamespaceView) namespaceJSON {
	return namespaceJSON{
		ID:                nv.Name,
		ForRuleSetVersion: nv.ForRuleSetVersion,
	}
}

type namespacesJSON struct {
	Version    int             `json:"version"`
	Namespaces []namespaceJSON `json:"namespaces"`
}

func newNamespacesJSON(nss *models.NamespacesView) namespacesJSON {
	views := make([]namespaceJSON, len(nss.Namespaces))
	for i, namespace := range nss.Namespaces {
		views[i] = newNamespaceJSON(namespace)
	}
	return namespacesJSON{
		Version:    nss.Version,
		Namespaces: views,
	}
}

type mappingRuleJSON struct {
	ID                  string          `json:"id,omitempty"`
	Name                string          `json:"name" validate:"required"`
	CutoverMillis       int64           `json:"cutoverMillis,omitempty"`
	Filter              string          `json:"filter" validate:"required"`
	Policies            []policy.Policy `json:"policies" validate:"required"`
	LastUpdatedBy       string          `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64           `json:"lastUpdatedAtMillis"`
}

func (m mappingRuleJSON) mappingRuleView() *models.MappingRuleView {
	return &models.MappingRuleView{
		ID:       m.ID,
		Name:     m.Name,
		Filter:   m.Filter,
		Policies: m.Policies,
	}
}

func newMappingRuleJSON(mrv *models.MappingRuleView) mappingRuleJSON {
	return mappingRuleJSON{
		ID:                  mrv.ID,
		Name:                mrv.Name,
		Filter:              mrv.Filter,
		Policies:            mrv.Policies,
		CutoverMillis:       mrv.CutoverNanos / nanosPerMilli,
		LastUpdatedBy:       mrv.LastUpdatedBy,
		LastUpdatedAtMillis: mrv.LastUpdatedAtNanos / nanosPerMilli,
	}
}

type mappingRuleHistoryJSON struct {
	MappingRules []mappingRuleJSON `json:"mappingRules"`
}

func newMappingRuleHistoryJSON(hist []*models.MappingRuleView) mappingRuleHistoryJSON {
	views := make([]mappingRuleJSON, len(hist))
	for i, mappingRule := range hist {
		views[i] = newMappingRuleJSON(mappingRule)
	}
	return mappingRuleHistoryJSON{MappingRules: views}
}

type rollupTargetJSON struct {
	Name     string          `json:"name" validate:"required"`
	Tags     []string        `json:"tags" validate:"required"`
	Policies []policy.Policy `json:"policies" validate:"required"`
}

func (t rollupTargetJSON) rollupTargetView() models.RollupTargetView {
	return models.RollupTargetView{
		Name:     t.Name,
		Tags:     t.Tags,
		Policies: t.Policies,
	}
}

func newRollupTargetJSON(t models.RollupTargetView) rollupTargetJSON {
	return rollupTargetJSON{
		Name:     t.Name,
		Tags:     t.Tags,
		Policies: t.Policies,
	}
}

type rollupRuleJSON struct {
	ID                  string             `json:"id,omitempty"`
	Name                string             `json:"name" validate:"required"`
	Filter              string             `json:"filter" validate:"required"`
	Targets             []rollupTargetJSON `json:"targets" validate:"required,dive,required"`
	CutoverMillis       int64              `json:"cutoverMillis,omitempty"`
	LastUpdatedBy       string             `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64              `json:"lastUpdatedAtMillis"`
}

func newRollupRuleJSON(rrv *models.RollupRuleView) rollupRuleJSON {
	targets := make([]rollupTargetJSON, len(rrv.Targets))
	for i, t := range rrv.Targets {
		targets[i] = newRollupTargetJSON(t)
	}
	return rollupRuleJSON{
		ID:                  rrv.ID,
		Name:                rrv.Name,
		Filter:              rrv.Filter,
		Targets:             targets,
		CutoverMillis:       rrv.CutoverNanos / nanosPerMilli,
		LastUpdatedBy:       rrv.LastUpdatedBy,
		LastUpdatedAtMillis: rrv.LastUpdatedAtNanos / nanosPerMilli,
	}
}

func (r rollupRuleJSON) rollupRuleView() *models.RollupRuleView {
	targets := make([]models.RollupTargetView, len(r.Targets))
	for i, t := range r.Targets {
		targets[i] = t.rollupTargetView()
	}

	return &models.RollupRuleView{
		ID:      r.ID,
		Name:    r.Name,
		Filter:  r.Filter,
		Targets: targets,
	}
}

type rollupRuleHistoryJSON struct {
	RollupRules []rollupRuleJSON `json:"rollupRules"`
}

func newRollupRuleHistoryJSON(hist []*models.RollupRuleView) rollupRuleHistoryJSON {
	views := make([]rollupRuleJSON, len(hist))
	for i, rollupRule := range hist {
		views[i] = newRollupRuleJSON(rollupRule)
	}
	return rollupRuleHistoryJSON{RollupRules: views}
}

type ruleSetJSON struct {
	Namespace     string            `json:"id"`
	Version       int               `json:"version"`
	CutoverMillis int64             `json:"cutoverMillis"`
	MappingRules  []mappingRuleJSON `json:"mappingRules"`
	RollupRules   []rollupRuleJSON  `json:"rollupRules"`
}

func newRuleSetJSON(latest *models.RuleSetSnapshotView) ruleSetJSON {
	var mrJSON []mappingRuleJSON
	for _, m := range latest.MappingRules {
		mrJSON = append(mrJSON, newMappingRuleJSON(m))
	}
	var rrJSON []rollupRuleJSON
	for _, r := range latest.RollupRules {
		rrJSON = append(rrJSON, newRollupRuleJSON(r))
	}
	return ruleSetJSON{
		Namespace:     latest.Namespace,
		Version:       latest.Version,
		CutoverMillis: latest.CutoverNanos / nanosPerMilli,
		MappingRules:  mrJSON,
		RollupRules:   rrJSON,
	}
}

type idGenType int

const (
	generateID idGenType = iota
	dontGenerateID
)

// ruleSetSnapshot create a RuleSetSnapshot from a rulesetJSON. If the ruleSetJSON has no IDs
// for any of its mapping rules or rollup rules, it generates missing IDs and sets as a string UUID
// string so they can be stored in a mapping (id -> rule).
func (r ruleSetJSON) ruleSetSnapshot(idGenType idGenType) (*models.RuleSetSnapshotView, error) {
	mappingRules := make(map[string]*models.MappingRuleView, len(r.MappingRules))
	for _, mr := range r.MappingRules {
		id := mr.ID
		if id == "" {
			if idGenType == dontGenerateID {
				return nil, fmt.Errorf("can't convert rulesetJSON to ruleSetSnapshot, no mapping rule id for %v", mr)
			}
			id = uuid.New()
			mr.ID = id
		}
		mappingRules[id] = mr.mappingRuleView()
	}

	rollupRules := make(map[string]*models.RollupRuleView, len(r.RollupRules))
	for _, rr := range r.RollupRules {
		id := rr.ID
		if id == "" {
			if idGenType == dontGenerateID {
				return nil, fmt.Errorf("can't convert rulesetJSON to ruleSetSnapshot, no rollup rule id for %v", rr)
			}
			id = uuid.New()
			rr.ID = id
		}
		rollupRules[id] = rr.rollupRuleView()
	}

	return &models.RuleSetSnapshotView{
		Namespace:    r.Namespace,
		Version:      r.Version,
		MappingRules: mappingRules,
		RollupRules:  rollupRules,
	}, nil
}
