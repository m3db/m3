package tagfiltertree

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

type Rule struct {
	TagFilters []string
	Namespace  string
}

func (r *Rule) Resolve(inputTags map[string]string) (string, error) {
	if !IsVarTagValue(r.Namespace) {
		return r.Namespace, nil
	}

	varMap := make(map[string]string)
	for _, tagFilter := range r.TagFilters {
		tags, err := TagsFromTagFilter(tagFilter)
		if err != nil {
			return "", err
		}
		for _, tag := range tags {
			if tag.Var != "" {
				// is var tag
				inputTagValue, ok := inputTags[tag.Name]
				if !ok {
					return "", errors.New("tag not found")
				}
				varMap[tag.Var] = inputTagValue
			}
		}
	}

	ns := r.Namespace
	for varName, varValue := range varMap {
		ns = strings.ReplaceAll(ns, varName, varValue)
	}
	return ns, nil
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
			name: "multiple input tags, multiple filters, multiple rules, multiple values, match",
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
						"tag1:val1 tag4:value4 tag5:value5",
					},
					Namespace: "namespaceA",
				},
				{
					TagFilters: []string{
						"tag1:val* tag4:value4 tag5:value5",
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
			name: "single rule, all negation, match",
			inputTags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag4": "value4",
				"tag5": "value5",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag3:!*",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{"namespace1"},
		},
		{
			name: "single rule, all negation, no match",
			inputTags: map[string]string{
				"tag1": "value1",
				"tag2": "value2",
				"tag4": "value4",
				"tag5": "value5",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag2:!*",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{},
		},
		{
			name: "single rule, negation, match",
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
						"tag3:!{value1,value2}",
					},
					Namespace: "namespace1",
				},
			},
			expected: []string{"namespace1"},
		},
		{
			name: "multiple rules, negation, single match",
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
						"tag1:!* tag2:value2",
					},
					Namespace: "namespace1",
				},
				{
					TagFilters: []string{
						"tag1:value1 tag6:!*",
					},
					Namespace: "namespace2",
				},
			},
			expected: []string{"namespace2"},
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
		{
			name: "multiple input tags, var tag value, single rule match",
			inputTags: map[string]string{
				"tag3": "apple",
				"tag4": "banana",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag3:{{VAR1}} tag4:{{VAR2}}",
						"tag5:value5 tag6:value6",
					},
					Namespace: "{{VAR1}}_{{VAR2}}",
				},
			},
			expected: []string{"apple_banana"},
		},
		{
			name: "multiple input tags, var tag value, multi rule match",
			inputTags: map[string]string{
				"tag3": "apple",
				"tag4": "banana",
				"tag5": "train",
				"tag6": "car",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag3:{{VAR1}} tag4:{{VAR2}}",
						"tag5:value5 tag6:value6",
					},
					Namespace: "namespace1_{{VAR1}}_{{VAR2}}",
				},
				{
					TagFilters: []string{
						"tag4:{{VAR5}} tag5:{{VAR6}} tag6:{{VAR2}}",
						"tag5:value5 tag7:value7",
					},
					Namespace: "{{VAR6}}_{{VAR6}}_{{VAR2}}_{{VAR5}}",
				},
			},
			expected: []string{
				"namespace1_apple_banana",
				"train_train_car_banana",
			},
		},
		{
			name: "multiple input tags, wildcard tag value, multi rule match",
			inputTags: map[string]string{
				"tag3": "apple",
				"tag4": "banana",
				"tag5": "train",
				"tag6": "car",
			},
			rules: []Rule{
				{
					TagFilters: []string{
						"tag3:app* tag4:ban*",
						"tag5:train tag6:value6",
					},
					Namespace: "namespace1",
				},
				{
					TagFilters: []string{
						"tag4:bag* tag5:* tag6:c*",
						"tag5:value5 tag7:value7",
					},
					Namespace: "namespace2",
				},
			},
			expected: []string{"namespace1"},
		},
	}

	less := func(a, b string) bool { return a < b }
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := New[*Rule]()
			for _, rule := range tt.rules {
				for _, tagFilter := range rule.TagFilters {
					err := tree.AddTagFilter(tagFilter, &rule)
					require.NoError(t, err)
				}
			}

			actual, err := tree.Match(tt.inputTags)
			resolved := make([]string, 0, len(actual))
			for _, r := range actual {
				ns, err := r.Resolve(tt.inputTags)
				require.NoError(t, err)
				resolved = append(resolved, ns)
			}

			require.NoError(t, err)
			if len(tt.expected) == 0 {
				require.Empty(t, actual)
				return
			}

			actualNamespaces := uniqueNamespaces(resolved)
			require.Equal(t, "", cmp.Diff(tt.expected, actualNamespaces, cmpopts.SortSlices(less)))
		})
	}
}

func uniqueNamespaces(input []string) []string {
	unique := make(map[string]struct{})
	for _, r := range input {
		unique[r] = struct{}{}
	}

	output := make([]string, 0, len(unique))
	for ns, _ := range unique {
		output = append(output, ns)
	}
	return output
}
