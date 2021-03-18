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

	"github.com/m3db/m3/src/ctl/service/r2"
	r2store "github.com/m3db/m3/src/ctl/service/r2/store"
	"github.com/m3db/m3/src/metrics/aggregation"
	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/rules/view/changes"
	"github.com/m3db/m3/src/x/clock"

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

	proto, err := initialRuleSet.ToMutableRuleSet().Proto()
	require.NoError(t, err)
	expected, err := rules.NewRuleSetFromProto(1, proto, rules.NewOptions())
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
		expectedProto, err := expectedMutable.Proto()
		require.NoError(t, err)
		rsProto, err := rs.Proto()
		require.NoError(t, err)
		require.Equal(t, expectedProto, rsProto)
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
		view.MappingRules{},
		view.RollupRules{},
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

func TestUpdateRuleSetFetchNotFound(t *testing.T) {
	rsChanges := newTestRuleSetChanges(
		view.MappingRules{},
		view.RollupRules{},
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockedStore := rules.NewMockStore(ctrl)
	mockedStore.EXPECT().ReadRuleSet("testNamespace").Return(
		nil,
		merrors.NewNotFoundError("something bad has happened"),
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
	require.IsType(t, r2.NewNotFoundError(""), err)
}

func TestUpdateRuleSetFetchFailure(t *testing.T) {
	rsChanges := newTestRuleSetChanges(
		view.MappingRules{},
		view.RollupRules{},
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
		view.MappingRules{
			"invalidMappingRule": []view.MappingRule{},
		},
		view.RollupRules{},
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

	proto, err := initialRuleSet.ToMutableRuleSet().Proto()
	require.NoError(t, err)
	expected, err := rules.NewRuleSetFromProto(1, proto, rules.NewOptions())
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
		expectedProto, err := expectedMutable.Proto()
		require.NoError(t, err)
		rsProto, err := rs.Proto()
		require.NoError(t, err)
		require.Equal(t, expectedProto, rsProto)
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

func newTestRuleSetChanges(mrs view.MappingRules, rrs view.RollupRules) changes.RuleSetChanges {
	mrChanges := make([]changes.MappingRuleChange, 0, len(mrs))
	for uuid := range mrs {
		mrChanges = append(
			mrChanges,
			changes.MappingRuleChange{
				Op:     changes.ChangeOp,
				RuleID: &uuid,
				RuleData: &view.MappingRule{
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
				RuleData: &view.RollupRule{
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
	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testTarget",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min),
	)
	if err != nil {
		return nil, err
	}

	err = mutable.ApplyRuleSetChanges(
		changes.RuleSetChanges{
			Namespace: "testNamespace",
			RollupRuleChanges: []changes.RollupRuleChange{
				{
					Op: changes.AddOp,
					RuleData: &view.RollupRule{
						Name: "rollupRule3",
						Targets: []view.RollupTarget{
							{
								Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
									{
										Type:   pipeline.RollupOpType,
										Rollup: rr1,
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
			MappingRuleChanges: []changes.MappingRuleChange{
				{
					Op: changes.AddOp,
					RuleData: &view.MappingRule{
						Name: "mappingRule3",
						StoragePolicies: policy.StoragePolicies{
							policy.MustParseStoragePolicy("1s:6h"),
						},
					},
				},
			},
		},
		meta,
	)
	if err != nil {
		return nil, err
	}
	proto, err := mutable.Proto()
	if err != nil {
		return nil, err
	}
	ruleSet, err := rules.NewRuleSetFromProto(version, proto, rules.NewOptions())
	if err != nil {
		return nil, err
	}

	return ruleSet, nil
}

func newEmptyTestRuleSet(version int, meta rules.UpdateMetadata) (rules.RuleSet, error) {
	proto, err := rules.NewEmptyRuleSet("testNamespace", meta).Proto()
	if err != nil {
		return nil, err
	}
	ruleSet, err := rules.NewRuleSetFromProto(version, proto, rules.NewOptions())
	if err != nil {
		return nil, err
	}

	return ruleSet, nil
}
