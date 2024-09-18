package attribution

import (
	"reflect"
	"sort"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/metrics/attribution/rules"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtest "github.com/m3db/m3/src/x/test"
	"github.com/stretchr/testify/require"
)

func TestAttributorNamespaces(t *testing.T) {
	tests := []struct {
		name       string
		inputRules []*rules.Rule
		metricID   []byte
		expectedNS []string
	}{
		{
			name: "single namespace, single filter",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{"tag1:value1"}),
			},
			metricID: []byte("tag1=value1,tag2=value2"),
			expectedNS: []string{
				"foo+",
				"global+",
			},
		},
		{
			name: "single rule, single filter with secondary ns",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetSecondaryNS("foo2").
					SetTagFilters([]string{"tag1:value1"}),
			},
			metricID: []byte("tag1=value1,tag2=value2"),
			expectedNS: []string{
				"foo+foo2",
				"global+",
			},
		},
		{
			name: "single rule, multiple filters",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{
						"tag1:value1 tag2:value2",
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
					}),
			},
			metricID: []byte("tag1=value1,tag2=value2"),
			expectedNS: []string{
				"foo+",
				"global+",
			},
		},
		{
			name: "multiple rules, single filter",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{
						"tag1:value1",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetTagFilters([]string{
						"tag2:value2",
					}),
			},
			metricID: []byte("tag1=value1,tag2=value2"),
			expectedNS: []string{
				"foo+",
				"bar+",
				"global+",
			},
		},
		{
			name: "multiple rules, multiple filters",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{
						"tag1:value1 tag2:value2",
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetTagFilters([]string{
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
						"tag4:value4 tag5:value5",
					}),
			},
			metricID: []byte("tag1=value1,tag2=value2,tag3=value3"),
			expectedNS: []string{
				"foo+",
				"bar+",
				"global+",
			},
		},
		{
			name: "multiple rules with secondary ns, multiple filters",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetSecondaryNS("foo2").
					SetTagFilters([]string{
						"tag1:value1 tag2:value2",
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetSecondaryNS("bar2").
					SetTagFilters([]string{
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
						"tag4:value4 tag5:value5",
					}),
			},
			metricID: []byte("tag1=value1,tag2=value2,tag3=value3"),
			expectedNS: []string{
				"foo+foo2",
				"bar+bar2",
				"global+",
			},
		},
		{
			name: "multiple rules with secondary ns, multiple filters, composite tag values",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetSecondaryNS("foo2").
					SetTagFilters([]string{
						"tag1:value1 tag2:{value2,apple}",
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetSecondaryNS("bar2").
					SetTagFilters([]string{
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
						"tag4:value4 tag5:value5",
					}),
			},
			metricID: []byte("tag1=value1,tag2=apple,tag3=value3"),
			expectedNS: []string{
				"foo+foo2",
				//	"bar+bar2",
				"global+",
			},
		},
		{
			name: "multiple rules with secondary ns, multiple filters, var tag values",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("{{VAR1}}").
					SetSecondaryNS("foo2").
					SetTagFilters([]string{
						"tag1:value1 tag2:{{VAR1}}",
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetSecondaryNS("bar2").
					SetTagFilters([]string{
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
						"tag4:value4 tag5:value5",
					}),
			},
			metricID: []byte("tag1=value1,tag2=apple,tag3=value3"),
			expectedNS: []string{
				"apple+foo2",
				"global+",
			},
		},
		{
			name: "multiple rules with secondary ns, multiple filters, multi var tag values",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("{{VAR1}}").
					SetSecondaryNS("foo2").
					SetTagFilters([]string{
						"tag1:value1 tag2:{{VAR1}}",
						"tag2:value2 tag3:value3",
						"tag3:value3 tag4:value4",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetSecondaryNS("{{VAR1}}_{{VAR2}}").
					SetTagFilters([]string{
						"tag2:value2 tag3:{{VAR1}}",
						"tag3:value3 tag4:{{VAR2}}",
						"tag4:value4 tag5:value5",
					}),
			},
			metricID: []byte("tag1=value1,tag2=apple,tag3=value3,tag4=value4"),
			expectedNS: []string{
				"apple+foo2",
				"bar+value3_value4",
				"global+",
			},
		},
		{
			name: "no matched rules",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{
						"tag1:value1",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetTagFilters([]string{
						"tag2:value2",
					}),
			},
			metricID: []byte("tag3=value3"),
			expectedNS: []string{
				"global+",
			},
		},
		{
			name: "no filters",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetTagFilters([]string{}),
			},
			metricID: []byte("tag1=value1,tag2=value2"),
			expectedNS: []string{
				"global+",
			},
		},
		{
			name: "no matched rules, no filters",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{
						"tag1:value1",
					}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetTagFilters([]string{
						"tag2:value2",
					}),
			},
			metricID: []byte("tag3=value3"),
			expectedNS: []string{
				"global+",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newTestAttributorWithRuleSet(t, newRuleSetFromRules(tt.inputRules))
			err := a.Init()
			require.NoError(t, err)

			results, err := a.Match(tt.metricID)
			// prepare actual NS string.
			actualNamespaces := make([]string, 0)
			for _, res := range results {
				actualNamespaces = append(
					actualNamespaces,
					res.PrimaryNS+"+"+res.SecondaryNS,
				)
			}
			require.NoError(t, err)
			if len(tt.expectedNS) == 0 {
				require.Empty(t, actualNamespaces)
				return
			}
			sort.Strings(tt.expectedNS)
			sort.Strings(actualNamespaces)
			require.True(t, reflect.DeepEqual(tt.expectedNS, actualNamespaces))
			a.Close()
		})
	}
}

func TestAttributorUpdate(t *testing.T) {
	tests := []struct {
		name                        string
		inputRules                  []*rules.Rule
		updateRules                 []*rules.Rule
		metricID                    []byte
		expectedNamespacesForInput  []string
		expectedNamespacesForUpdate []string
	}{
		{
			name: "add rule",
			inputRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{"tag1:value1"}),
			},
			updateRules: []*rules.Rule{
				rules.NewRule().
					SetName("foo").
					SetPrimaryNS("foo").
					SetTagFilters([]string{"tag1:value1"}),
				rules.NewRule().
					SetName("bar").
					SetPrimaryNS("bar").
					SetTagFilters([]string{"tag2:value2"}),
			},
			metricID: []byte("tag1=value1,tag2=value2"),
			expectedNamespacesForInput: []string{
				"foo+",
				"global+",
			},
			expectedNamespacesForUpdate: []string{
				"foo+",
				"bar+",
				"global+",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newTestAttributorWithRuleSet(t, newRuleSetFromRules(tt.inputRules))
			err := a.Init()
			require.NoError(t, err)
			results, err := a.Match(tt.metricID)
			require.NoError(t, err)

			actualNamespaces := make([]string, 0)
			for _, r := range results {
				actualNamespaces = append(
					actualNamespaces,
					r.PrimaryNS+"+"+r.SecondaryNS,
				)
			}
			sort.Strings(tt.expectedNamespacesForInput)
			sort.Strings(actualNamespaces)
			require.True(
				t,
				reflect.DeepEqual(tt.expectedNamespacesForInput, actualNamespaces),
				"incorrect owners for input, expected: %v, actual: %v",
				tt.expectedNamespacesForInput,
				actualNamespaces,
			)

			err = a.Update(newRuleSetFromRules(tt.updateRules))
			require.NoError(t, err)
			results, err = a.Match(tt.metricID)
			require.NoError(t, err)
			actualNamespaces = actualNamespaces[:0]
			for _, r := range results {
				actualNamespaces = append(
					actualNamespaces,
					r.PrimaryNS+"+"+r.SecondaryNS,
				)
			}
			sort.Strings(tt.expectedNamespacesForUpdate)
			sort.Strings(actualNamespaces)
			require.True(
				t,
				reflect.DeepEqual(tt.expectedNamespacesForUpdate, actualNamespaces),
				"incorrect owners for update, expected: %v, actual: %v",
				tt.expectedNamespacesForUpdate,
				actualNamespaces,
			)
		})
	}
}

func TestAttributorInit(t *testing.T) {
	defer leaktest.Check(t)()

	customRuleSet := rules.NewRuleSet().SetRules(
		[]*rules.Rule{
			rules.NewRule().
				SetName("test-rule").
				SetPrimaryNS("foo").
				SetTagFilters([]string{"tag1:value1"}),
		},
	)

	customRuleSet.SetName("test-ruleset")
	a := newTestAttributorWithRuleSet(t, customRuleSet)

	err := a.Init()
	require.NoError(t, err)

	a.Close()
}

func newRuleSetFromRules(ruleList []*rules.Rule) rules.RuleSet {
	return rules.NewRuleSet().SetRules(ruleList)
}

func newTagsFilterOptions() filters.TagsFilterOptions {
	poolOpts := pool.NewObjectPoolOptions()
	sortedTagIteratorPool := id.NewSortedTagIteratorPool(poolOpts)
	sortedTagIteratorPool.Init(func() id.SortedTagIterator {
		return m3.NewPooledSortedTagIterator(nil, sortedTagIteratorPool)
	})
	sortedTagIteratorFn := func(tagPairs []byte) id.SortedTagIterator {
		it := sortedTagIteratorPool.Get()
		it.Reset(tagPairs)
		return it
	}

	tagFilterOptions := filters.TagsFilterOptions{
		NameTagKey:          []byte("name"),
		NameAndTagsFn:       m3.NameAndTags,
		SortedTagIteratorFn: sortedTagIteratorFn,
	}

	return tagFilterOptions
}

func newTestAttributorWithRuleSet(
	t *testing.T,
	ruleSet rules.RuleSet,
) Attributor {
	ctrl := xtest.NewController(t)
	store := mem.NewStore()
	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(gomock.Any()).Return(store, nil)

	rs, err := rules.NewService(rules.NewServiceOptions().SetConfigService(cs))
	require.NoError(t, err)

	instrumentOpts := instrument.NewOptions()
	opts := NewOptions().
		SetTagFilterOptions(newTagsFilterOptions()).
		SetRulesService(rs).
		SetCustomRulesKVKey("test-ruleset").
		SetInstrumentOptions(instrumentOpts).
		SetKVWatchInitTimeout(5).
		SetDefaultRuleMatcher(newTestDefaultRuleMatcher())

	ruleSet.SetName("test-ruleset")

	_, err = rs.CheckAndSet(ruleSet, 0)
	require.NoError(t, err)

	return NewAttributor(opts)
}

type testDefaultRuleMatcher struct {
}

func (m *testDefaultRuleMatcher) Match(_ []byte) []rules.Result {
	return []rules.Result{
		{
			Rule: rules.NewRule().
				SetName("global").
				SetPrimaryNS("global"),
			PrimaryNS:   "global",
			SecondaryNS: "",
		},
	}
}

func (m *testDefaultRuleMatcher) Update(_ rules.RuleSet) error {
	return nil
}

func newTestDefaultRuleMatcher() RuleMatcher {
	return &testDefaultRuleMatcher{}
}
