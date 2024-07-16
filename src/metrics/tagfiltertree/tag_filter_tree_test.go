package tagfiltertree

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

type Rule struct {
	TagFilters []string
	Namespace  string
}

type ResolvedRule struct {
	*Rule
	VarMap map[string]string
}

func (rr *ResolvedRule) Annotate(varMap map[string]string) *ResolvedRule {
	return &ResolvedRule{
		Rule:   rr.Rule,
		VarMap: varMap,
	}
}

func TestTreeGetData(t *testing.T) {
	tests := []struct {
		name      string
		inputTags map[string]string
		rules     []Rule
		expected  []string
	}{
		{
			name: "multiple input tags, multiple filters, multiple rules, match",
			inputTags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
				"tag4": "value4",
				"tag5": "value5",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag1:value1 tag2:value2",
					},
					Namespace: "namespace1",
				},
				{
					TagFilters: []string{
						"tag4:value4 tag5:value5",
					},
					Namespace: "namespace2",
				},
				{
					TagFilters: []string{
						"tag5:*",
					},
					Namespace: "namespace3",
				},
				{
					TagFilters: []string{
						"tag8:value8 tag9:value9",
						"tag5:value5 tag6:value6",
					},
					Namespace: "namespace4",
				},
			},
			expected: []string{"namespace1", "namespace2", "namespace3"},
		},
		{
			name: "single input tag, single filter, match",
			inputTags: map[string]string{
				"tag1": "value1",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag1:value1",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{"namespace1"},
		},
		{
			name: "multiple input tags, single filter, match",
			inputTags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag1:value1",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{"namespace1"},
		},
		{
			name: "single input tag, multiple filters, match",
			inputTags: map[string]string{
				"tag1": "value1",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag1:value1",
						"tag2:value2",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{"namespace1"},
		},
		{
			name: "single input tag, multiple filters, no match",
			inputTags: map[string]string{
				"tag1": "value1",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag3:value1",
						"tag2:value2",
					},
					Namespace: "namespace1",
				},
			},
			expected: nil,
		},
		{
			name: "multiple input tags, multiple filters, match",
			inputTags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag3": "value3",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag1:value1 tag2:value2",
						"tag4:value2 tag5:value2",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{"namespace1"},
		},
		{
			name: "multiple input tags, multiple filters, no match",
			inputTags: map[string]string{
				"tag3": "value3",
				"tag4": "value4",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag1:value1 tag2:value2",
						"tag5:value5 tag6:value6",
					},
					Namespace: "namespace1",
				},
				{
					TagFilters: []string{
						"tag7:*",
					},
					Namespace: "namespace2",
				},
			},
			expected: nil,
		},
		{
			name: "multiple input tags, composite tag value, match",
			inputTags: map[string]string{
				"tag3": "apple",
				"tag4": "banana",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag3:{value3,apple} tag4:{value4,banana}",
						"tag5:value5 tag6:value6",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{"namespace1"},
		},
	}

	less := func(a, b string) bool { return a < b }
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := New[*ResolvedRule]()
			for _, rule := range tt.rules {
				for _, tagFilter := range rule.TagFilters {
					tags, err := TagsFromTagFilter(tagFilter)
					require.NoError(t, err)
					localRule := rule
					tree.AddTagFilter(tags, &ResolvedRule{
						Rule: &localRule,
					})
				}
			}

			actual := tree.Match(tt.inputTags)
			if len(tt.expected) == 0 {
				require.Empty(t, actual)
				return
			}

			actualNamespaces := uniqueNamespaces(actual)
			require.Equal(t, "", cmp.Diff(tt.expected, actualNamespaces, cmpopts.SortSlices(less)))
		})
	}
}

func TestParseTagValue(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expected  []string
		shouldErr bool
	}{
		{
			name:      "simple tag value",
			input:     "value",
			expected:  []string{"value"},
			shouldErr: false,
		},
		{
			name:      "simple tag value, error",
			input:     "val{ue",
			expected:  []string{},
			shouldErr: true,
		},
		{
			name:      "composite tag value",
			input:     "  {foo,bar,  baz}",
			expected:  []string{"foo", "bar", "baz"},
			shouldErr: false,
		},
		{
			name:      "composite tag value, error",
			input:     "{foo,bar,baz}}",
			expected:  []string{},
			shouldErr: true,
		},
		{
			name:      "var tag value",
			input:     "  {{foo}}",
			expected:  []string{"{{foo}}"},
			shouldErr: false,
		},
		{
			name:      "var tag value, error",
			input:     "  {{foo,bar}}",
			expected:  []string{},
			shouldErr: true,
		},
		{
			name:      "empty tag value",
			input:     "",
			expected:  []string{},
			shouldErr: true,
		},
		{
			name:      "empty composite tag value",
			input:     "{}",
			expected:  []string{},
			shouldErr: true,
		},
		{
			name:      "empty var tag value",
			input:     "{{}}",
			expected:  []string{},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := parseTagValue(tt.input)
			if tt.shouldErr {
				require.Error(t, err)
				return
			}

			sort.Strings(tt.expected)
			sort.Strings(actual)
			require.True(t, reflect.DeepEqual(tt.expected, actual))
		})
	}
}

func uniqueNamespaces(input []*ResolvedRule) []string {
	unique := make(map[string]*ResolvedRule)
	for _, s := range input {
		unique[s.Namespace] = s
	}

	output := make([]string, 0, len(unique))
	for _, a := range unique {
		output = append(output, a.Namespace)
	}
	return output
}
