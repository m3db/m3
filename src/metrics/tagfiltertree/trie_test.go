package tagfiltertree

import (
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

type Pattern struct {
	pattern string
	data    string
}

func TestTrieMatch(t *testing.T) {
	tests := []struct {
		name          string
		patterns      []Pattern
		input         string
		expectedMatch bool
		expectedData  []string
		isDisabled    bool
	}{
		{
			name: "suffix wildcard minimum match",
			patterns: []Pattern{
				{
					pattern: "foo*",
					data:    "data1",
				},
			},
			input:         "foo",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "suffix wildcard match",
			patterns: []Pattern{
				{
					pattern: "foo*",
					data:    "data1",
				},
			},
			input:         "foo123",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "suffix wildcard suffix match",
			patterns: []Pattern{
				{
					pattern: "foo*",
					data:    "data1",
				},
			},
			input:         "foofoofoofoo",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "suffix wildcard no match",
			patterns: []Pattern{
				{
					pattern: "foo*",
					data:    "data1",
				},
			},
			input:         "fobar",
			expectedMatch: false,
			expectedData:  []string{},
		},
		{
			name: "prefix wildcard minimum match",
			patterns: []Pattern{
				{
					pattern: "*foo",
					data:    "data1",
				},
			},
			input:         "foo",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "prefix wildcard prefix match",
			patterns: []Pattern{
				{
					pattern: "*foo",
					data:    "data1",
				},
			},
			input:         "fofofofoo",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "prefix wildcard no match",
			patterns: []Pattern{
				{
					pattern: "*foo",
					data:    "data1",
				},
			},
			input:         "barfobo",
			expectedMatch: false,
			expectedData:  []string{},
		},
		{
			name: "single exact match",
			patterns: []Pattern{
				{
					pattern: "foobar",
					data:    "data1",
				},
			},
			input:         "foobar",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "single suffix and prefix wildcard match",
			patterns: []Pattern{
				{
					pattern: "*foo*",
					data:    "data1",
				},
			},
			input:         "barfoobar",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "single negation match",
			patterns: []Pattern{
				{
					pattern: "!foo",
					data:    "data1",
				},
			},
			input:         "bar",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "single negation with wildcard match",
			patterns: []Pattern{
				{
					pattern: "!*foo*",
					data:    "data1",
				},
			},
			input:         "fofobar",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "composite, absolute match, single match",
			patterns: []Pattern{
				{
					pattern: "{foo,bar}",
					data:    "data1",
				},
			},
			input:         "bar",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "composite starting middle, wildcards, single match",
			patterns: []Pattern{
				{
					pattern: "*choco{*foo*,*bar*}late*",
					data:    "data1",
				},
			},
			input:         "123chocobarlate456",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "composite, wildcard match, single match",
			patterns: []Pattern{
				{
					pattern: "{*foo*,*bar*}",
					data:    "data1",
				},
			},
			input:         "bar",
			expectedMatch: true,
			expectedData:  []string{"data1"},
		},
		{
			name: "single negative composite, wildcards, no match",
			patterns: []Pattern{
				{
					pattern: "!{*foo*,*bar*}",
					data:    "data1",
				},
			},
			input:         "bar",
			expectedMatch: false,
			expectedData:  []string{},
		},
		{
			name: "multiple composite, wildcard and absolute match, single match",
			patterns: []Pattern{
				{
					pattern: "{*foo*,*bal*,batbaz}",
					data:    "data1",
				},
				{
					pattern: "*batbaz",
					data:    "data2",
				},
			},
			input:         "batbaz",
			expectedMatch: true,
			expectedData:  []string{"data1", "data2"},
		},
		{
			name: "multiple intersecting composite, wildcard and absolute match, all match",
			patterns: []Pattern{
				{
					pattern: "!{*foo*,*bal*}",
					data:    "data1",
				},
				{
					pattern: "!{*bal*,gaz*}",
					data:    "data2",
				},
			},
			input:         "batbaz",
			expectedMatch: true,
			expectedData:  []string{"data1", "data2"},
			// disable test since we don't support multiple negative composite filters at the moment.
			isDisabled: true,
		},
		{
			name: "multiple intersecting composite, wildcard and absolute match, single match",
			patterns: []Pattern{
				{
					pattern: "!{*foo*,*bal*}",
					data:    "data1",
				},
				{
					pattern: "!{*bal*,gaz*}",
					data:    "data2",
				},
			},
			input:         "foo",
			expectedMatch: true,
			expectedData:  []string{"data2"},
			// disable test since we don't support multiple negative composite filters at the moment.
			isDisabled: true,
		},
		{
			name: "multiple intersecting composite, wildcard and absolute match, no match",
			patterns: []Pattern{
				{
					pattern: "!{*foo*,*bal*}",
					data:    "data1",
				},
				{
					pattern: "!{*bal*,gaz*}",
					data:    "data2",
				},
			},
			input:         "banbalabal",
			expectedMatch: false,
			expectedData:  []string{},
			// disable test since we don't support multiple negative composite filters at the moment.
			isDisabled: true,
		},
		{
			name: "multiple negations with wildcard, single match",
			patterns: []Pattern{
				{
					pattern: "!*foo*",
					data:    "data1",
				},
				{
					pattern: "!*bar*",
					data:    "data2",
				},
				{
					pattern: "!*baz*",
					data:    "data3",
				},
			},
			input:         "fofoobar",
			expectedMatch: true,
			expectedData:  []string{"data3"},
			// disable test since we don't support multiple negative composite filters at the moment.
			isDisabled: true,
		},
	}

	for _, tt := range tests {
		runTest := func(withData bool) {
			if tt.isDisabled {
				return
			}
			trie := NewTrie[string]()
			for _, rule := range tt.patterns {
				var data *string
				if withData {
					data = &rule.data
				}
				trie.Insert(rule.pattern, data)
			}

			var data []string
			if withData {
				data = make([]string, 0)
			}
			matched, err := trie.Match(tt.input, &data)
			require.NoError(t, err)

			// check match.
			require.Equal(t, tt.expectedMatch, matched)

			if !withData {
				for i := range data {
					require.Empty(t, data[i])
				}
				return
			}

			if len(tt.expectedData) == 0 {
				require.Empty(t, data)
				return
			}

			// dedup data.
			data = deduplicateData(data)

			// check data.
			sort.Slice(data, func(i, j int) bool {
				return data[i] < data[j]
			})
			sort.Slice(tt.expectedData, func(i, j int) bool {
				return tt.expectedData[i] < tt.expectedData[j]
			})

			require.Equal(t, true, reflect.DeepEqual(data, tt.expectedData))
		}

		t.Run(tt.name+"_with_data", func(t *testing.T) {
			runTest(true)
		})
		t.Run(tt.name+"_without_data", func(t *testing.T) {
			runTest(false)
		})
	}
}

func deduplicateData(data []string) []string {
	dataMap := make(map[string]struct{})
	for i := range data {
		dataMap[data[i]] = struct{}{}
	}
	data = make([]string, 0, len(dataMap))
	for k := range dataMap {
		data = append(data, k)
	}
	return data
}
