// Copyright (c) 2018 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3ctl/service/r2"
	r2store "github.com/m3db/m3ctl/service/r2/store"
	merrors "github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3metrics/rules/models"
	"github.com/m3db/m3metrics/rules/models/changes"
	"github.com/m3db/m3x/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestUpdateRuleSet(t *testing.T) {
	helper := rules.NewRuleSetUpdateHelper(time.Minute)
	initialRuleSet, err := testRuleSet(1, helper.NewUpdateMetadata(100, "validUser"))
	require.NoError(t, err)

	mrs, err := initialRuleSet.MappingRules()
	require.NoError(t, err)
	rrs, err := initialRuleSet.RollupRules()
	require.NoError(t, err)
	rsChanges := newTestRuleSetChanges(mrs, rrs)
	require.NoError(t, err)

	schema, err := initialRuleSet.ToMutableRuleSet().Schema()
	require.NoError(t, err)
	expected, err := rules.NewRuleSetFromSchema(1, schema, rules.NewOptions())
	require.NoError(t, err)
	expectedMutable := expected.ToMutableRuleSet()
	err = expectedMutable.ApplyRuleSetChanges(rsChanges, helper.NewUpdateMetadata(200, "validUser"))
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedStore := rules.NewMockStore(ctrl)
	mockedStore.EXPECT().ReadRuleSet("testNamespace").Return(
		initialRuleSet,
		nil,
	).Times(2)

	mockedStore.EXPECT().WriteRuleSet(gomock.Any()).Do(func(rs rules.MutableRuleSet) {
		// mock library can not match rules.MutableRuleSet interface so use this function
		expectedSchema, err := expectedMutable.Schema()
		require.NoError(t, err)
		rsSchema, err := rs.Schema()
		require.NoError(t, err)
		require.Equal(t, expectedSchema, rsSchema)
	}).Return(nil)

	storeOpts := NewStoreOptions().SetClockOptions(
		clock.NewOptions().SetNowFn(func() time.Time {
			return time.Unix(0, 200)
		}),
	)
	rulesStore := NewStore(mockedStore, storeOpts)
	uOpts := r2store.NewUpdateOptions().SetAuthor("validUser")
	_, err = rulesStore.UpdateRuleSet(rsChanges, 1, uOpts)
	require.NoError(t, err)
}

func TestUpdateRuleSetVersionMisMatch(t *testing.T) {
	helper := rules.NewRuleSetUpdateHelper(time.Minute)
	initialRuleSet, err := newEmptyTestRuleSet(2, helper.NewUpdateMetadata(100, "validUser"))
	require.NoError(t, err)

	rsChanges := newTestRuleSetChanges(
		models.MappingRuleViews{},
		models.RollupRuleViews{},
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedStore := rules.NewMockStore(ctrl)
	mockedStore.EXPECT().ReadRuleSet("testNamespace").Return(
		initialRuleSet,
		nil,
	)

	storeOpts := NewStoreOptions().SetClockOptions(
		clock.NewOptions().SetNowFn(func() time.Time {
			return time.Unix(0, 200)
		}),
	)
	rulesStore := NewStore(mockedStore, storeOpts)
	uOpts := r2store.NewUpdateOptions().SetAuthor("validUser")
	_, err = rulesStore.UpdateRuleSet(rsChanges, 1, uOpts)
	require.Error(t, err)
	require.IsType(t, r2.NewConflictError(""), err)
}

func TestUpdateRuleSetFetchFailure(t *testing.T) {
	rsChanges := newTestRuleSetChanges(
		models.MappingRuleViews{},
		models.RollupRuleViews{},
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedStore := rules.NewMockStore(ctrl)
	mockedStore.EXPECT().ReadRuleSet("testNamespace").Return(
		nil,
		merrors.NewValidationError("something bad has happened"),
	)

	storeOpts := NewStoreOptions().SetClockOptions(
		clock.NewOptions().SetNowFn(func() time.Time {
			return time.Unix(0, 200)
		}),
	)
	rulesStore := NewStore(mockedStore, storeOpts)
	uOpts := r2store.NewUpdateOptions().SetAuthor("validUser")
	_, err := rulesStore.UpdateRuleSet(rsChanges, 1, uOpts)
	require.Error(t, err)
	require.IsType(t, r2.NewBadInputError(""), err)
}

func TestUpdateRuleSetMutationFail(t *testing.T) {
	helper := rules.NewRuleSetUpdateHelper(time.Minute)
	initialRuleSet, err := newEmptyTestRuleSet(1, helper.NewUpdateMetadata(100, "validUser"))

	rsChanges := newTestRuleSetChanges(
		models.MappingRuleViews{
			"invalidMappingRule": []*models.MappingRuleView{},
		},
		models.RollupRuleViews{},
	)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedStore := rules.NewMockStore(ctrl)
	mockedStore.EXPECT().ReadRuleSet("testNamespace").Return(
		initialRuleSet,
		nil,
	)

	storeOpts := NewStoreOptions().SetClockOptions(
		clock.NewOptions().SetNowFn(func() time.Time {
			return time.Unix(0, 200)
		}),
	)
	rulesStore := NewStore(mockedStore, storeOpts)
	uOpts := r2store.NewUpdateOptions().SetAuthor("validUser")
	_, err = rulesStore.UpdateRuleSet(rsChanges, 1, uOpts)
	require.Error(t, err)
	require.IsType(t, r2.NewConflictError(""), err)
}

func TestUpdateRuleSetWriteFailure(t *testing.T) {
	helper := rules.NewRuleSetUpdateHelper(time.Minute)
	initialRuleSet, err := testRuleSet(1, helper.NewUpdateMetadata(100, "validUser"))
	require.NoError(t, err)

	mrs, err := initialRuleSet.MappingRules()
	require.NoError(t, err)
	rrs, err := initialRuleSet.RollupRules()
	require.NoError(t, err)
	rsChanges := newTestRuleSetChanges(mrs, rrs)
	require.NoError(t, err)

	schema, err := initialRuleSet.ToMutableRuleSet().Schema()
	require.NoError(t, err)
	expected, err := rules.NewRuleSetFromSchema(1, schema, rules.NewOptions())
	require.NoError(t, err)
	expectedMutable := expected.ToMutableRuleSet()
	err = expectedMutable.ApplyRuleSetChanges(rsChanges, helper.NewUpdateMetadata(200, "validUser"))
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedStore := rules.NewMockStore(ctrl)
	mockedStore.EXPECT().ReadRuleSet("testNamespace").Return(
		initialRuleSet,
		nil,
	)

	mockedStore.EXPECT().WriteRuleSet(gomock.Any()).Do(func(rs rules.MutableRuleSet) {
		// mock library can not match rules.MutableRuleSet interface so use this function
		expectedSchema, err := expectedMutable.Schema()
		require.NoError(t, err)
		rsSchema, err := rs.Schema()
		require.NoError(t, err)
		require.Equal(t, expectedSchema, rsSchema)
	}).Return(merrors.NewStaleDataError("something has gone wrong"))

	storeOpts := NewStoreOptions().SetClockOptions(
		clock.NewOptions().SetNowFn(func() time.Time {
			return time.Unix(0, 200)
		}),
	)
	rulesStore := NewStore(mockedStore, storeOpts)
	uOpts := r2store.NewUpdateOptions().SetAuthor("validUser")
	_, err = rulesStore.UpdateRuleSet(rsChanges, 1, uOpts)
	require.Error(t, err)
	require.IsType(t, r2.NewConflictError(""), err)
}

func newTestRuleSetChanges(mrs models.MappingRuleViews, rrs models.RollupRuleViews) changes.RuleSetChanges {
	mrChanges := make([]changes.MappingRuleChange, 0, len(mrs))
	for uuid := range mrs {
		mrChanges = append(
			mrChanges,
			changes.MappingRuleChange{
				Op:     changes.ChangeOp,
				RuleID: &uuid,
				RuleData: &models.MappingRule{
					ID:   uuid,
					Name: "updateMappingRule",
				},
			},
		)
	}

	rrChanges := make([]changes.RollupRuleChange, 0, len(rrs))
	for uuid := range rrs {
		rrChanges = append(
			rrChanges,
			changes.RollupRuleChange{
				Op:     changes.ChangeOp,
				RuleID: &uuid,
				RuleData: &models.RollupRule{
					ID:   uuid,
					Name: "updateRollupRule",
				},
			},
		)
	}

	return changes.RuleSetChanges{
		Namespace:          "testNamespace",
		RollupRuleChanges:  rrChanges,
		MappingRuleChanges: mrChanges,
	}
}

// nolint: unparam
func testRuleSet(version int, meta rules.UpdateMetadata) (rules.RuleSet, error) {
	mutable := rules.NewEmptyRuleSet("testNamespace", meta)
	err := mutable.ApplyRuleSetChanges(
		changes.RuleSetChanges{
			Namespace: "testNamespace",
			RollupRuleChanges: []changes.RollupRuleChange{
				changes.RollupRuleChange{
					Op: changes.AddOp,
					RuleData: &models.RollupRule{
						Name: "rollupRule3",
					},
				},
			},
			MappingRuleChanges: []changes.MappingRuleChange{
				changes.MappingRuleChange{
					Op: changes.AddOp,
					RuleData: &models.MappingRule{
						Name: "mappingRule3",
					},
				},
			},
		},
		meta,
	)
	if err != nil {
		return nil, err
	}
	schema, err := mutable.Schema()
	if err != nil {
		return nil, err
	}
	ruleSet, err := rules.NewRuleSetFromSchema(version, schema, rules.NewOptions())
	if err != nil {
		return nil, err
	}

	return ruleSet, nil
}

func newEmptyTestRuleSet(version int, meta rules.UpdateMetadata) (rules.RuleSet, error) {
	schema, err := rules.NewEmptyRuleSet("testNamespace", meta).Schema()
	if err != nil {
		return nil, err
	}
	ruleSet, err := rules.NewRuleSetFromSchema(version, schema, rules.NewOptions())
	if err != nil {
		return nil, err
	}

	return ruleSet, nil
}
