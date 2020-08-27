// Copyright (c) 2019 Uber Technologies, Inc.
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

package handleroptions

import (
	"encoding/json"
	"testing"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func toStrip(strs ...string) [][]byte {
	b := make([][]byte, 0, len(strs))
	for _, s := range strs {
		b = append(b, []byte(s))
	}

	return b
}

func mustMatcher(n string, v string, t models.MatchType) models.Matcher {
	m, err := models.NewMatcher(t, []byte(n), []byte(v))
	if err != nil {
		panic(err)
	}

	return m
}

func TestParse(t *testing.T) {
	tests := []struct {
		json          string
		expected      *storage.RestrictByTag
		expectedError bool
	}{
		{"{}", nil, false},
		{
			`{
			"match":[
				{"name":"a", "value":"b", "type":"EQUAL"},
				{"name":"c", "value":"d", "type":"NOTEQUAL"},
				{"name":"e", "value":"f", "type":"REGEXP"},
				{"name":"g", "value":"h", "type":"NOTREGEXP"},
				{"name":"i", "type":"EXISTS"},
				{"name":"j", "type":"NOTEXISTS"}
			]
		}`,
			&storage.RestrictByTag{
				Restrict: models.Matchers{
					mustMatcher("a", "b", models.MatchEqual),
					mustMatcher("c", "d", models.MatchNotEqual),
					mustMatcher("e", "f", models.MatchRegexp),
					mustMatcher("g", "h", models.MatchNotRegexp),
					mustMatcher("i", "", models.MatchField),
					mustMatcher("j", "", models.MatchNotField),
				},
				Strip: toStrip("a", "c", "e", "g", "i", "j"),
			},
			false,
		},
		{
			`{
			"match":[
				{"name":"a", "value":"b", "type":"EQUAL"},
				{"name":"c", "value":"d", "type":"NOTEQUAL"},
				{"name":"e", "value":"f", "type":"REGEXP"},
				{"name":"g", "value":"h", "type":"NOTREGEXP"},
				{"name":"i", "value":"j", "type":"EXISTS"},
				{"name":"k", "value":"l", "type":"NOTEXISTS"}
			],
			"strip":["foo"]
		}`,
			&storage.RestrictByTag{
				Restrict: models.Matchers{
					mustMatcher("a", "b", models.MatchEqual),
					mustMatcher("c", "d", models.MatchNotEqual),
					mustMatcher("e", "f", models.MatchRegexp),
					mustMatcher("g", "h", models.MatchNotRegexp),
					mustMatcher("i", "j", models.MatchField),
					mustMatcher("k", "l", models.MatchNotField),
				},
				Strip: toStrip("foo"),
			},
			false,
		},
		{
			`{"match":[{"name":"i", "value":"j", "type":"EXISTS"}],"strip":[]}`,
			&storage.RestrictByTag{
				Restrict: models.Matchers{
					mustMatcher("i", "j", models.MatchField),
				},
				Strip: [][]byte{},
			},
			false,
		},
		{
			`{"strip":["foo"]}`,
			&storage.RestrictByTag{
				Strip: toStrip("foo"),
			},
			false,
		},
		{`{"match":[{}]}`, nil, true},
		{`{"match":[{"type":"ALL"}]}`, nil, true},
		{`{"match":[{"type":"invalid"}]}`, nil, true},
	}

	for _, tt := range tests {
		var opts StringTagOptions
		err := json.Unmarshal([]byte(tt.json), &opts)
		require.NoError(t, err)

		validateErr := opts.Validate()
		if tt.expectedError {
			require.Error(t, validateErr)
		} else {
			require.NoError(t, validateErr)
		}

		a, err := opts.StorageOptions()
		if tt.expectedError {
			require.Error(t, err)
			require.Nil(t, a)
		} else {
			require.NoError(t, err)
			require.Equal(t, tt.expected, a)
		}
	}
}

func TestTagMapperValidate(t *testing.T) {
	tm := TagMapper{}
	assert.Error(t, tm.Validate())

	tm.Write = WriteOp{Tag: "foo", Value: "bar"}
	assert.NoError(t, tm.Validate())

	tm.Drop = DropOp{Tag: "foo"}
	assert.Error(t, tm.Validate())
}

func TestOpIsEmpty(t *testing.T) {
	t.Run("Append", func(t *testing.T) {
		op := WriteOp{}
		assert.True(t, op.IsEmpty())
		op.Tag = "foo"
		assert.False(t, op.IsEmpty())
	})

	t.Run("Drop", func(t *testing.T) {
		op := DropOp{}
		assert.True(t, op.IsEmpty())
		op.Tag = "foo"
		assert.False(t, op.IsEmpty())
	})

	t.Run("DropWithValue", func(t *testing.T) {
		op := DropWithValueOp{}
		assert.True(t, op.IsEmpty())
		op.Value = "foo"
		assert.False(t, op.IsEmpty())
	})

	t.Run("Replace", func(t *testing.T) {
		op := ReplaceOp{}
		assert.True(t, op.IsEmpty())
		op.Tag = "foo"
		assert.False(t, op.IsEmpty())
	})
}
