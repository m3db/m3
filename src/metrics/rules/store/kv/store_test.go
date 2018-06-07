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

package kv

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3cluster/kv/mem"
	merrors "github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3metrics/generated/proto/policypb"
	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3metrics/rules/models"

	"github.com/stretchr/testify/require"
)

const (
	testNamespaceKey  = "testKey"
	testNamespace     = "fooNs"
	testRuleSetKeyFmt = "rules/%s"
)

var (
	testNamespaces = &rulepb.Namespaces{
		Namespaces: []*rulepb.Namespace{
			&rulepb.Namespace{
				Name: "fooNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					&rulepb.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&rulepb.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			&rulepb.Namespace{
				Name: "barNs",
				Snapshots: []*rulepb.NamespaceSnapshot{
					&rulepb.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&rulepb.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        true,
					},
				},
			},
		},
	}

	testRuleSetKey = fmt.Sprintf(testRuleSetKeyFmt, testNamespace)
	testRuleSet    = &rulepb.RuleSet{
		Uuid:               "ruleset",
		Namespace:          "fooNs",
		CreatedAtNanos:     1234,
		LastUpdatedAtNanos: 5678,
		Tombstoned:         false,
		CutoverNanos:       34923,
		MappingRules: []*rulepb.MappingRule{
			&rulepb.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*rulepb.MappingRuleSnapshot{
					&rulepb.MappingRuleSnapshot{
						Name:         "foo",
						Tombstoned:   false,
						CutoverNanos: 12345,
						Filter:       "tag1:value1 tag2:value2",
						Policies: []*policypb.Policy{
							&policypb.Policy{
								StoragePolicy: &policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(24 * time.Hour),
									},
								},
								AggregationTypes: []aggregationpb.AggregationType{
									aggregationpb.AggregationType_P999,
								},
							},
						},
					},
					&rulepb.MappingRuleSnapshot{
						Name:         "foo",
						Tombstoned:   false,
						CutoverNanos: 67890,
						Filter:       "tag3:value3 tag4:value4",
						Policies: []*policypb.Policy{
							&policypb.Policy{
								StoragePolicy: &policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(24 * time.Hour),
									},
								},
							},
							&policypb.Policy{
								StoragePolicy: &policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(5 * time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &policypb.Retention{
										Period: int64(48 * time.Hour),
									},
								},
							},
						},
					},
				},
			},
			&rulepb.MappingRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*rulepb.MappingRuleSnapshot{
					&rulepb.MappingRuleSnapshot{
						Name:         "dup",
						Tombstoned:   false,
						CutoverNanos: 12345,
						Filter:       "tag1:value1 tag2:value2",
						Policies: []*policypb.Policy{
							&policypb.Policy{
								StoragePolicy: &policypb.StoragePolicy{
									Resolution: &policypb.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &policypb.Retention{
										Period: int64(24 * time.Hour),
									},
								},
								AggregationTypes: []aggregationpb.AggregationType{
									aggregationpb.AggregationType_P999,
								},
							},
						},
					},
				},
			},
		},
		RollupRules: []*rulepb.RollupRule{
			&rulepb.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*rulepb.RollupRuleSnapshot{
					&rulepb.RollupRuleSnapshot{
						Name:         "foo2",
						Tombstoned:   false,
						CutoverNanos: 12345,
						Filter:       "tag1:value1 tag2:value2",
						Targets: []*rulepb.RollupTarget{
							&rulepb.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*policypb.Policy{
									&policypb.Policy{
										StoragePolicy: &policypb.StoragePolicy{
											Resolution: &policypb.Resolution{
												WindowSize: int64(10 * time.Second),
												Precision:  int64(time.Second),
											},
											Retention: &policypb.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
								},
							},
						},
					},
					&rulepb.RollupRuleSnapshot{
						Name:         "bar",
						Tombstoned:   true,
						CutoverNanos: 67890,
						Filter:       "tag3:value3 tag4:value4",
						Targets: []*rulepb.RollupTarget{
							&rulepb.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*policypb.Policy{
									&policypb.Policy{
										StoragePolicy: &policypb.StoragePolicy{
											Resolution: &policypb.Resolution{
												WindowSize: int64(time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &policypb.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
									&policypb.Policy{
										StoragePolicy: &policypb.StoragePolicy{
											Resolution: &policypb.Resolution{
												WindowSize: int64(5 * time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &policypb.Retention{
												Period: int64(48 * time.Hour),
											},
										},
										AggregationTypes: []aggregationpb.AggregationType{
											aggregationpb.AggregationType_MEAN,
										},
									},
								},
							},
						},
					},
				},
			},
			&rulepb.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*rulepb.RollupRuleSnapshot{
					&rulepb.RollupRuleSnapshot{
						Name:         "foo",
						Tombstoned:   false,
						CutoverNanos: 12345,
						Filter:       "tag1:value1 tag2:value2",
						Targets: []*rulepb.RollupTarget{
							&rulepb.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*policypb.Policy{
									&policypb.Policy{
										StoragePolicy: &policypb.StoragePolicy{
											Resolution: &policypb.Resolution{
												WindowSize: int64(10 * time.Second),
												Precision:  int64(time.Second),
											},
											Retention: &policypb.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
								},
							},
						},
					},
					&rulepb.RollupRuleSnapshot{
						Name:         "baz",
						Tombstoned:   false,
						CutoverNanos: 67890,
						Filter:       "tag3:value3 tag4:value4",
						Targets: []*rulepb.RollupTarget{
							&rulepb.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*policypb.Policy{
									&policypb.Policy{
										StoragePolicy: &policypb.StoragePolicy{
											Resolution: &policypb.Resolution{
												WindowSize: int64(time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &policypb.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
									&policypb.Policy{
										StoragePolicy: &policypb.StoragePolicy{
											Resolution: &policypb.Resolution{
												WindowSize: int64(5 * time.Minute),
												Precision:  int64(time.Minute),
											},
											Retention: &policypb.Retention{
												Period: int64(48 * time.Hour),
											},
										},
										AggregationTypes: []aggregationpb.AggregationType{
											aggregationpb.AggregationType_MEAN,
										},
									},
								},
							},
						},
					},
				},
			},
			&rulepb.RollupRule{
				Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
				Snapshots: []*rulepb.RollupRuleSnapshot{
					&rulepb.RollupRuleSnapshot{
						Name:         "dup",
						Tombstoned:   false,
						CutoverNanos: 12345,
						Filter:       "tag1:value1 tag2:value2",
						Targets: []*rulepb.RollupTarget{
							&rulepb.RollupTarget{
								Name: "rName1",
								Tags: []string{"rtagName1", "rtagName2"},
								Policies: []*policypb.Policy{
									&policypb.Policy{
										StoragePolicy: &policypb.StoragePolicy{
											Resolution: &policypb.Resolution{
												WindowSize: int64(10 * time.Second),
												Precision:  int64(time.Second),
											},
											Retention: &policypb.Retention{
												Period: int64(24 * time.Hour),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
)

func TestRuleSetKey(t *testing.T) {
	s := testStore()
	defer s.Close()

	key := s.(*store).ruleSetKey(testNamespace)
	require.Equal(t, "rules/fooNs", key)
}

func TestNewStore(t *testing.T) {
	opts := NewStoreOptions(testNamespaceKey, testRuleSetKeyFmt, nil)
	kvStore := mem.NewStore()
	s := NewStore(kvStore, opts).(*store)
	defer s.Close()

	require.Equal(t, s.kvStore, kvStore)
	require.Equal(t, s.opts, opts)
}

func TestReadNamespaces(t *testing.T) {
	s := testStore()
	defer s.Close()

	_, e := s.(*store).kvStore.Set(testNamespaceKey, testNamespaces)
	require.NoError(t, e)
	nss, err := s.ReadNamespaces()
	require.NoError(t, err)
	require.NotNil(t, nss.Namespaces)
}

func TestReadNamespacesError(t *testing.T) {
	s := testStore()
	defer s.Close()

	_, e := s.(*store).kvStore.Set(testNamespaceKey, &rulepb.RollupRule{Uuid: "x"})
	require.NoError(t, e)
	nss, err := s.ReadNamespaces()
	require.Error(t, err)
	require.Nil(t, nss)
}

func TestReadRuleSet(t *testing.T) {
	s := testStore()
	defer s.Close()

	_, e := s.(*store).kvStore.Set(testRuleSetKey, testRuleSet)
	require.NoError(t, e)
	rs, err := s.ReadRuleSet(testNamespace)
	require.NoError(t, err)
	require.NotNil(t, rs)
}

func TestReadRuleSetError(t *testing.T) {
	s := testStore()
	defer s.Close()

	_, e := s.(*store).kvStore.Set(testRuleSetKey, &rulepb.Namespace{Name: "x"})
	require.NoError(t, e)
	rs, err := s.ReadRuleSet("blah")
	require.Error(t, err)
	require.Nil(t, rs)
}

func TestWriteAll(t *testing.T) {
	s := testStore()
	defer s.Close()

	rs, err := s.ReadRuleSet(testNamespaceKey)
	require.Error(t, err)
	require.Nil(t, rs)

	nss, err := s.ReadNamespaces()
	require.Error(t, err)
	require.Nil(t, nss)

	mutable := newMutableRuleSetFromProto(t, 0, testRuleSet)
	namespaces, err := rules.NewNamespaces(0, testNamespaces)
	require.NoError(t, err)

	err = s.WriteAll(&namespaces, mutable)
	require.NoError(t, err)

	rs, err = s.ReadRuleSet(testNamespace)
	require.NoError(t, err)
	rsProto, err := rs.ToMutableRuleSet().Proto()
	require.NoError(t, err)
	require.Equal(t, rsProto, testRuleSet)

	nss, err = s.ReadNamespaces()
	require.NoError(t, err)
	nssProto, err := nss.Proto()
	require.NoError(t, err)
	require.Equal(t, nssProto, testNamespaces)
}

func TestWriteAllValidationError(t *testing.T) {
	errInvalidRuleSet := errors.New("invalid ruleset")
	v := &mockValidator{
		validateFn: func(rules.RuleSet) error { return errInvalidRuleSet },
	}
	s := testStoreWithValidator(v)
	defer s.Close()
	require.Equal(t, errInvalidRuleSet, s.WriteAll(nil, nil))
}

func TestWriteAllError(t *testing.T) {
	s := testStore()
	defer s.Close()

	rs, err := s.ReadRuleSet(testNamespaceKey)
	require.Error(t, err)
	require.Nil(t, rs)

	nss, err := s.ReadNamespaces()
	require.Error(t, err)
	require.Nil(t, nss)

	mutable := newMutableRuleSetFromProto(t, 1, testRuleSet)
	namespaces, err := rules.NewNamespaces(0, testNamespaces)
	require.NoError(t, err)

	type dataPair struct {
		nss *rules.Namespaces
		rs  rules.MutableRuleSet
	}

	otherNss, err := rules.NewNamespaces(1, testNamespaces)
	require.NoError(t, err)

	badPairs := []dataPair{
		dataPair{nil, nil},
		dataPair{nil, mutable},
		dataPair{&namespaces, nil},
		dataPair{&otherNss, mutable},
	}

	for _, p := range badPairs {
		err = s.WriteAll(p.nss, p.rs)
		require.Error(t, err)
	}

	_, err = s.ReadRuleSet(testNamespace)
	require.Error(t, err)

	_, err = s.ReadNamespaces()
	require.Error(t, err)
}

func TestWriteRuleSetValidationError(t *testing.T) {
	errInvalidRuleSet := errors.New("invalid ruleset")
	v := &mockValidator{
		validateFn: func(rules.RuleSet) error { return errInvalidRuleSet },
	}
	s := testStoreWithValidator(v)
	defer s.Close()
	require.Equal(t, errInvalidRuleSet, s.WriteRuleSet(nil))
}

func TestWriteRuleSetError(t *testing.T) {
	s := testStore()
	defer s.Close()

	rs, err := s.ReadRuleSet(testNamespaceKey)
	require.Error(t, err)
	require.Nil(t, rs)

	nss, err := s.ReadNamespaces()
	require.Error(t, err)
	require.Nil(t, nss)

	mutable := newMutableRuleSetFromProto(t, 1, testRuleSet)
	badRuleSets := []rules.MutableRuleSet{mutable, nil}
	for _, rs := range badRuleSets {
		err = s.WriteRuleSet(rs)
		require.Error(t, err)
	}

	err = s.WriteRuleSet(nil)
	require.Error(t, err)

	_, err = s.ReadRuleSet(testNamespace)
	require.Error(t, err)
}

func TestWriteRuleSetStaleDataError(t *testing.T) {
	s := testStore()
	defer s.Close()

	mutable := newMutableRuleSetFromProto(t, 0, testRuleSet)
	err := s.WriteRuleSet(mutable)
	require.NoError(t, err)

	jumpRuleSet := newMutableRuleSetFromProto(t, 5, testRuleSet)
	err = s.WriteRuleSet(jumpRuleSet)
	require.Error(t, err)
	require.IsType(t, merrors.NewStaleDataError(""), err)
}

func TestWriteAllNoNamespace(t *testing.T) {
	s := testStore()
	defer s.Close()

	rs, err := s.ReadRuleSet(testNamespaceKey)
	require.Error(t, err)
	require.Nil(t, rs)

	nss, err := s.ReadNamespaces()
	require.Error(t, err)
	require.Nil(t, nss)

	mutable := newMutableRuleSetFromProto(t, 0, testRuleSet)
	namespaces, err := rules.NewNamespaces(0, testNamespaces)
	require.NoError(t, err)

	err = s.WriteAll(&namespaces, mutable)
	require.NoError(t, err)

	rs, err = s.ReadRuleSet(testNamespace)
	require.NoError(t, err)

	_, err = s.ReadNamespaces()
	require.NoError(t, err)

	err = s.WriteRuleSet(rs.ToMutableRuleSet())
	require.NoError(t, err)

	rs, err = s.ReadRuleSet(testNamespace)
	require.NoError(t, err)
	nss, err = s.ReadNamespaces()
	require.NoError(t, err)
	require.Equal(t, nss.Version(), 1)
	require.Equal(t, rs.Version(), 2)
}
func TestWriteAllStaleDataError(t *testing.T) {
	s := testStore()
	defer s.Close()

	mutable := newMutableRuleSetFromProto(t, 0, testRuleSet)
	namespaces, err := rules.NewNamespaces(0, testNamespaces)
	require.NoError(t, err)

	err = s.WriteAll(&namespaces, mutable)
	require.NoError(t, err)

	jumpNamespaces, err := rules.NewNamespaces(5, testNamespaces)
	require.NoError(t, err)
	err = s.WriteAll(&jumpNamespaces, mutable)
	require.Error(t, err)
	require.IsType(t, merrors.NewStaleDataError(""), err)
}

func testStore() rules.Store {
	return testStoreWithValidator(nil)
}

func testStoreWithValidator(validator rules.Validator) rules.Store {
	opts := NewStoreOptions(testNamespaceKey, testRuleSetKeyFmt, validator)
	kvStore := mem.NewStore()
	return NewStore(kvStore, opts)
}

// newMutableRuleSetFromProto creates a new MutableRuleSet from a proto object.
func newMutableRuleSetFromProto(
	t *testing.T,
	version int,
	rs *rulepb.RuleSet,
) rules.MutableRuleSet {
	// Takes a blank Options stuct because none of the mutation functions need the options.
	roRuleSet, err := rules.NewRuleSetFromProto(version, rs, rules.NewOptions())
	require.NoError(t, err)
	return roRuleSet.ToMutableRuleSet()
}

type validateFn func(rs rules.RuleSet) error

type mockValidator struct {
	validateFn validateFn
}

func (v *mockValidator) Validate(rs rules.RuleSet) error                             { return v.validateFn(rs) }
func (v *mockValidator) ValidateSnapshot(snapshot *models.RuleSetSnapshotView) error { return nil }
func (v *mockValidator) Close()                                                      {}
