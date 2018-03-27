package r2

import (
	"testing"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestNewNamespaceJSON(t *testing.T) {
	id := "name"
	fixture := testNamespaceView(id)
	expected := namespaceJSON{
		ID:                id,
		ForRuleSetVersion: fixture.ForRuleSetVersion,
	}
	require.EqualValues(t, expected, newNamespaceJSON(fixture))
}

func TestNewNamespacesJSON(t *testing.T) {
	id1 := "name1"
	id2 := "name2"
	fixture := testNamespacesView(id1, id2)
	expected := namespacesJSON{
		Version: 1,
		Namespaces: []namespaceJSON{
			{
				ID:                id1,
				ForRuleSetVersion: 1,
			},
			{
				ID:                id2,
				ForRuleSetVersion: 1,
			},
		},
	}
	require.EqualValues(t, expected, newNamespacesJSON(fixture))
}

func TestNewMappingRuleJSON(t *testing.T) {
	id := "mr_id"
	name := "mr_name"
	fixture := testMappingRuleView(id, name)
	expected := mappingRuleJSON{
		ID:                  id,
		Name:                name,
		Filter:              "filter",
		Policies:            []policy.Policy{},
		CutoverMillis:       0,
		LastUpdatedBy:       "",
		LastUpdatedAtMillis: 0,
	}
	require.EqualValues(t, expected, newMappingRuleJSON(fixture))
}

func TestMappingRuleView(t *testing.T) {
	id := "id"
	name := "name"
	fixture := testMappingRuleJSON(id, name)
	expected := &models.MappingRuleView{
		ID:       id,
		Name:     name,
		Filter:   "filter",
		Policies: []policy.Policy{},
	}
	require.EqualValues(t, expected, fixture.mappingRuleView())
}

func TestNewMappingRuleHistoryJSON(t *testing.T) {
	id := "id"
	hist := []*models.MappingRuleView{
		testMappingRuleView(id, "name1"),
		testMappingRuleView(id, "name2"),
	}
	expected := mappingRuleHistoryJSON{
		MappingRules: []mappingRuleJSON{
			{
				ID:                  id,
				Name:                "name1",
				Filter:              "filter",
				Policies:            []policy.Policy{},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
			{
				ID:                  id,
				Name:                "name2",
				Filter:              "filter",
				Policies:            []policy.Policy{},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
		},
	}
	require.EqualValues(t, expected, newMappingRuleHistoryJSON(hist))
}

func TestRollupTargetView(t *testing.T) {
	fixture := testRollupTargetJSON("name")
	expected := models.RollupTargetView{
		Name:     "name",
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
	require.EqualValues(t, expected, fixture.rollupTargetView())
}

func TestNewRollupTargetJSON(t *testing.T) {
	fixture := testRollupTargetView("name")
	expected := rollupTargetJSON{
		Name:     "name",
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
	require.EqualValues(t, expected, newRollupTargetJSON(*fixture))
}

func TestNewRollupRuleJSON(t *testing.T) {
	targets := []models.RollupTargetView{
		*testRollupTargetView("target1"),
		*testRollupTargetView("target2"),
	}
	fixture := testRollupRuleView("rr_id", "rr_name", targets)
	expected := rollupRuleJSON{
		ID:     "rr_id",
		Name:   "rr_name",
		Filter: "filter",
		Targets: []rollupTargetJSON{
			{
				Name:     "target1",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
			{
				Name:     "target2",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
		},
		CutoverMillis:       0,
		LastUpdatedBy:       "",
		LastUpdatedAtMillis: 0,
	}
	require.EqualValues(t, expected, newRollupRuleJSON(fixture))
}

func TestRollupRuleView(t *testing.T) {
	targets := []rollupTargetJSON{
		*testRollupTargetJSON("target1"),
		*testRollupTargetJSON("target2"),
	}
	fixture := testRollupRuleJSON("id", "name", targets)
	expected := &models.RollupRuleView{
		ID:     "id",
		Name:   "name",
		Filter: "filter",
		Targets: []models.RollupTargetView{
			{
				Name:     "target1",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
			{
				Name:     "target2",
				Tags:     []string{"tag"},
				Policies: []policy.Policy{},
			},
		},
	}
	require.EqualValues(t, expected, fixture.rollupRuleView())
}

func TestNewRollupRuleHistoryJSON(t *testing.T) {
	id := "id"
	targets := []models.RollupTargetView{
		*testRollupTargetView("target1"),
		*testRollupTargetView("target2"),
	}
	hist := []*models.RollupRuleView{
		testRollupRuleView(id, "name1", targets),
		testRollupRuleView(id, "name2", targets),
	}
	expected := rollupRuleHistoryJSON{
		RollupRules: []rollupRuleJSON{
			{
				ID:     id,
				Name:   "name1",
				Filter: "filter",
				Targets: []rollupTargetJSON{
					{
						Name:     "target1",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
					{
						Name:     "target2",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
			{
				ID:     id,
				Name:   "name2",
				Filter: "filter",
				Targets: []rollupTargetJSON{
					{
						Name:     "target1",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
					{
						Name:     "target2",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
		},
	}
	require.EqualValues(t, expected, newRollupRuleHistoryJSON(hist))
}

func TestRuleSetSnapshot(t *testing.T) {
	mappingRules := []mappingRuleJSON{
		*testMappingRuleJSON("mr1_id", "mr1"),
		*testMappingRuleJSON("mr2_id", "mr2"),
	}
	rollupRules := []rollupRuleJSON{
		*testRollupRuleJSON("rr1_id", "rr1", []rollupTargetJSON{*testRollupTargetJSON("target1")}),
		*testRollupRuleJSON("rr2_id", "rr2", []rollupTargetJSON{*testRollupTargetJSON("target2")}),
	}
	fixture := testRuleSetJSON("rs_ns", mappingRules, rollupRules)
	expected := &models.RuleSetSnapshotView{
		Namespace:    "rs_ns",
		Version:      1,
		CutoverNanos: 0,
		MappingRules: map[string]*models.MappingRuleView{
			"mr1_id": {
				ID:       "mr1_id",
				Name:     "mr1",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
			"mr2_id": {
				ID:       "mr2_id",
				Name:     "mr2",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
		},
		RollupRules: map[string]*models.RollupRuleView{
			"rr1_id": {
				ID:     "rr1_id",
				Name:   "rr1",
				Filter: "filter",
				Targets: []models.RollupTargetView{
					{
						Name:     "target1",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
			"rr2_id": {
				ID:     "rr2_id",
				Name:   "rr2",
				Filter: "filter",
				Targets: []models.RollupTargetView{
					{
						Name:     "target2",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
		},
	}
	actual, err := fixture.ruleSetSnapshot(generateID)
	require.NoError(t, err)
	require.EqualValues(t, expected, actual)
}

func TestRuleSetSnapshotGenerateMissingID(t *testing.T) {
	mappingRules := []mappingRuleJSON{
		*testMappingRuleJSON("", "mr"),
		*testMappingRuleJSON("", "mr"),
	}
	rollupRules := []rollupRuleJSON{
		*testRollupRuleJSON("", "rr", []rollupTargetJSON{*testRollupTargetJSON("target")}),
		*testRollupRuleJSON("", "rr", []rollupTargetJSON{*testRollupTargetJSON("target")}),
	}
	fixture := testRuleSetJSON("namespace", mappingRules, rollupRules)

	actual, err := fixture.ruleSetSnapshot(generateID)
	require.NoError(t, err)
	mrIDs := []string{}
	rrIDs := []string{}

	// Test that generated IDs are UUIDs and add them to their respective lists for further testing.
	for id := range actual.MappingRules {
		require.NotNil(t, uuid.Parse(id))
		mrIDs = append(mrIDs, id)
	}
	for id := range actual.RollupRules {
		require.NotNil(t, uuid.Parse(id))
		rrIDs = append(rrIDs, id)
	}

	expected := &models.RuleSetSnapshotView{
		Namespace:    "namespace",
		Version:      1,
		CutoverNanos: 0,
		MappingRules: map[string]*models.MappingRuleView{
			mrIDs[0]: {
				ID:       mrIDs[0],
				Name:     "mr",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
			mrIDs[1]: {
				ID:       mrIDs[1],
				Name:     "mr",
				Filter:   "filter",
				Policies: []policy.Policy{},
			},
		},
		RollupRules: map[string]*models.RollupRuleView{
			rrIDs[0]: {
				ID:     rrIDs[0],
				Name:   "rr",
				Filter: "filter",
				Targets: []models.RollupTargetView{
					{
						Name:     "target",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
			rrIDs[1]: {
				ID:     rrIDs[1],
				Name:   "rr",
				Filter: "filter",
				Targets: []models.RollupTargetView{
					{
						Name:     "target",
						Tags:     []string{"tag"},
						Policies: []policy.Policy{},
					},
				},
			},
		},
	}
	require.EqualValues(t, expected, actual)
}

func TestRuleSetSnapshotFailMissingMappingRuleID(t *testing.T) {
	mappingRules := []mappingRuleJSON{
		*testMappingRuleJSON("", "mr"),
		*testMappingRuleJSON("id1", "mr"),
	}
	rollupRules := []rollupRuleJSON{
		*testRollupRuleJSON("id2", "rr", []rollupTargetJSON{*testRollupTargetJSON("target")}),
		*testRollupRuleJSON("id3", "rr", []rollupTargetJSON{*testRollupTargetJSON("target")}),
	}
	fixture := testRuleSetJSON("namespace", mappingRules, rollupRules)

	_, err := fixture.ruleSetSnapshot(dontGenerateID)
	require.Error(t, err)
}

func TestRuleSetSnapshotFailMissingRollupRuleID(t *testing.T) {
	mappingRules := []mappingRuleJSON{
		*testMappingRuleJSON("id1", "mr"),
		*testMappingRuleJSON("id2", "mr"),
	}
	rollupRules := []rollupRuleJSON{
		*testRollupRuleJSON("id3", "rr", []rollupTargetJSON{*testRollupTargetJSON("target")}),
		*testRollupRuleJSON("", "rr", []rollupTargetJSON{*testRollupTargetJSON("target")}),
	}
	fixture := testRuleSetJSON("namespace", mappingRules, rollupRules)

	_, err := fixture.ruleSetSnapshot(dontGenerateID)
	require.Error(t, err)
}

// Tests Setup
func testNamespaceView(name string) *models.NamespaceView {
	return &models.NamespaceView{
		Name:              name,
		ForRuleSetVersion: 1,
	}
}

func testNamespacesView(namespaceNames ...string) *models.NamespacesView {
	namespaces := make([]*models.NamespaceView, len(namespaceNames))
	for i, name := range namespaceNames {
		namespaces[i] = testNamespaceView(name)
	}
	return &models.NamespacesView{
		Version:    1,
		Namespaces: namespaces,
	}
}

func testMappingRuleView(id, name string) *models.MappingRuleView {
	return &models.MappingRuleView{
		ID:       id,
		Name:     name,
		Filter:   "filter",
		Policies: []policy.Policy{},
	}
}

func testMappingRuleJSON(id, name string) *mappingRuleJSON {
	return &mappingRuleJSON{
		ID:       id,
		Name:     name,
		Filter:   "filter",
		Policies: []policy.Policy{},
	}
}

func testRollupTargetJSON(name string) *rollupTargetJSON {
	return &rollupTargetJSON{
		Name:     name,
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
}

func testRollupTargetView(name string) *models.RollupTargetView {
	return &models.RollupTargetView{
		Name:     name,
		Tags:     []string{"tag"},
		Policies: []policy.Policy{},
	}
}

func testRollupRuleJSON(id, name string, targets []rollupTargetJSON) *rollupRuleJSON {
	return &rollupRuleJSON{
		ID:      id,
		Name:    name,
		Filter:  "filter",
		Targets: targets,
	}
}

func testRollupRuleView(id, name string, targets []models.RollupTargetView) *models.RollupRuleView {
	return &models.RollupRuleView{
		ID:      id,
		Name:    name,
		Filter:  "filter",
		Targets: targets,
	}
}

func testRuleSetJSON(namespace string, mappingRules []mappingRuleJSON,
	rollupRules []rollupRuleJSON) *ruleSetJSON {
	return &ruleSetJSON{
		Namespace:     namespace,
		Version:       1,
		CutoverMillis: 0,
		MappingRules:  mappingRules,
		RollupRules:   rollupRules,
	}
}
