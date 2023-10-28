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
// THE SOFTWARE.

package index

import (
	"fmt"
	"regexp/syntax"
	"strings"
	"testing"
	"unicode"

	"github.com/m3db/m3/src/x/tallytest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestEnsureSyntaxPerlTreatsAnchorsAsTextTerminator(t *testing.T) {
	// Test to ensure future compatibility with changes in `regexp/syntax`.
	//
	// We require that '^' and '$' only match input terminating characters (i.e.
	// text boundaries, not line boundaries within the input). The line of code
	// below ensures that syntax.Perl does the same.
	require.NotZero(t, syntax.Perl&syntax.OneLine)

	// ensure our `parseRegexp` internal function uses the right flags too.
	re, err := parseRegexp(".*")
	require.NoError(t, err)
	require.NotZero(t, re.Flags&syntax.OneLine)
}

func TestEnsureRegexpUnachoredee(t *testing.T) {
	ast, err := parseRegexp("(?:^abc$){0,4}")
	require.NoError(t, err)
	pprintAst(ast)
	println(fmt.Sprintf("%v", dumpRegexp(ast)))
}

func TestEnsureRegexpUnachored(t *testing.T) {
	testCases := []testCase{
		{
			name:           "naked ^",
			input:          "^",
			expectedOutput: "emp{}",
		},
		{
			name:           "naked $",
			input:          "$",
			expectedOutput: "emp{}",
		},
		{
			name:           "empty string ^$",
			input:          "^$",
			expectedOutput: "cat{}",
		},
		{
			name:           "invalid naked concat ^$",
			input:          "$^",
			expectedOutput: "cat{eot{}bot{}}",
		},
		{
			name:           "simple case of ^",
			input:          "^abc",
			expectedOutput: "str{abc}",
		},
		{
			name:           "simple case of $",
			input:          "abc$",
			expectedOutput: "str{abc}",
		},
		{
			name:           "simple case of both ^ & $",
			input:          "^abc$",
			expectedOutput: "str{abc}",
		},
		{
			name:           "weird case of internal ^",
			input:          "^a^bc$",
			expectedOutput: "cat{lit{a}bot{}str{bc}}",
		},
		{
			name:           "weird case of internal $",
			input:          "^a$bc$",
			expectedOutput: "cat{lit{a}eot{}str{bc}}",
		},
		{
			name:           "alternate of sub expressions with only legal ^ and $",
			input:          "(?:^abc$)|(?:^xyz$)",
			expectedOutput: "alt{str{abc}str{xyz}}",
		},
		{
			name:           "concat of sub expressions with only legal ^ and $",
			input:          "(^abc$)(?:^xyz$)",
			expectedOutput: "cat{cap{cat{str{abc}eot{}}}bot{}str{xyz}}",
		},
		{
			name:           "alternate of sub expressions with illegal ^ and $",
			input:          "(?:^a$bc$)|(?:^xyz$)",
			expectedOutput: "alt{cat{lit{a}eot{}str{bc}}str{xyz}}",
		},
		{
			name:           "concat of sub expressions with illegal ^ and $",
			input:          "(?:^a$bc$)(?:^xyz$)",
			expectedOutput: "cat{lit{a}eot{}str{bc}eot{}bot{}str{xyz}}",
		},
		{
			name:           "question mark case both boundaries success",
			input:          "(?:^abc$)?",
			expectedOutput: "que{str{abc}}",
		},
		{
			name:           "question mark case only ^",
			input:          "(?:^abc)?",
			expectedOutput: "que{str{abc}}",
		},
		{
			name:           "question mark case only $",
			input:          "(?:abc$)?",
			expectedOutput: "que{str{abc}}",
		},
		{
			name:           "question concat case $",
			input:          "abc$?",
			expectedOutput: "str{abc}",
		},
		{
			name:           "star mark case both boundaries success",
			input:          "(?:^abc$)*",
			expectedOutput: "cat{que{str{abc}}star{cat{bot{}str{abc}eot{}}}}",
		},
		{
			name:           "star mark case only ^",
			input:          "(?:^abc)*",
			expectedOutput: "cat{que{str{abc}}star{cat{bot{}str{abc}}}}",
		},
		{
			name:           "star mark case only $",
			input:          "(?:abc$)*",
			expectedOutput: "cat{que{str{abc}}star{cat{str{abc}eot{}}}}",
		},
		{
			name:           "star concat case $",
			input:          "abc$*",
			expectedOutput: "cat{str{abc}star{eot{}}}",
		},
		{
			name:           "star concat case ^",
			input:          "^*abc",
			expectedOutput: "cat{star{bot{}}str{abc}}",
		},
		{
			name:           "plus mark case both boundaries success",
			input:          "(?:^abc$)+",
			expectedOutput: "cat{str{abc}star{cat{bot{}str{abc}eot{}}}}",
		},
		{
			name:           "plus mark case with capturing group",
			input:          "(^abc$)+",
			expectedOutput: "cat{cap{str{abc}}star{cap{cat{bot{}str{abc}eot{}}}}}",
		},
		{
			name:           "plus mark case only ^",
			input:          "(?:^abc)+",
			expectedOutput: "cat{str{abc}star{cat{bot{}str{abc}}}}",
		},
		{
			name:           "plus mark case only $",
			input:          "(?:abc$)+",
			expectedOutput: "cat{str{abc}star{cat{str{abc}eot{}}}}",
		},
		{
			name:           "plus concat case $",
			input:          "abc$+",
			expectedOutput: "cat{str{abc}star{eot{}}}",
		},
		{
			name:           "plus concat case ^",
			input:          "^+abc",
			expectedOutput: "cat{star{bot{}}str{abc}}",
		},
		{
			name:           "repeat case both boundaries success",
			input:          "(?:^abc$){3,4}",
			expectedOutput: "cat{str{abc}rep{2,3 cat{bot{}str{abc}eot{}}}}",
		},
		{
			name:           "repeat case unbounded max",
			input:          "(?:^abc$){3,}",
			expectedOutput: "cat{str{abc}rep{2,-1 cat{bot{}str{abc}eot{}}}}",
		},
		{
			name:           "repeat case unbounded max with 1 min",
			input:          "(?:^abc$){1,2}",
			expectedOutput: "cat{str{abc}rep{0,1 cat{bot{}str{abc}eot{}}}}",
		},
		{
			name:           "repeat case unbounded max with 0 min",
			input:          "(?:^abc$){0,2}",
			expectedOutput: "rep{0,2 cat{bot{}str{abc}eot{}}}",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re, err := parseRegexp(tc.input)
			require.NoError(t, err)
			parsed, err := EnsureRegexpUnanchored(re)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedOutput, dumpRegexp(parsed))
		})
	}
}

func TestEnsureRegexpAnchored(t *testing.T) {
	testCases := []testCase{
		{
			name:           "naked ^",
			input:          "(?:)",
			expectedOutput: "cat{bot{}eot{\\z}}",
		},
		{
			name:           "invalid naked concat ^$",
			input:          "$^",
			expectedOutput: "cat{bot{}eot{}bot{}eot{\\z}}",
		},
		{
			name:           "simple case of literal",
			input:          "abc",
			expectedOutput: "cat{bot{}str{abc}eot{\\z}}",
		},
		{
			name:           "weird case of internal ^",
			input:          "a^bc",
			expectedOutput: "cat{bot{}lit{a}bot{}str{bc}eot{\\z}}",
		},
		{
			name:           "weird case of internal $",
			input:          "a$bc",
			expectedOutput: "cat{bot{}lit{a}eot{}str{bc}eot{\\z}}",
		},
		{
			name:           "alternate of sub expressions with only legal ^ and $",
			input:          "abc|xyz",
			expectedOutput: "cat{bot{}alt{str{abc}str{xyz}}eot{\\z}}",
		},
		{
			name:           "concat of sub expressions with only legal ^ and $",
			input:          "(?:abc)(?:xyz)",
			expectedOutput: "cat{bot{}str{abcxyz}eot{\\z}}",
		},
		{
			name:           "question mark case both boundaries success",
			input:          "(?:abc)?",
			expectedOutput: "cat{bot{}que{str{abc}}eot{\\z}}",
		},
		{
			name:           "star mark case both boundaries success",
			input:          "(?:abc)*",
			expectedOutput: "cat{bot{}star{str{abc}}eot{\\z}}",
		},
		{
			name:           "plus mark case both boundaries success",
			input:          "(?:abc)+",
			expectedOutput: "cat{bot{}plus{str{abc}}eot{\\z}}",
		},
		{
			name:           "repeat case both boundaries success",
			input:          "(?:abc){3,4}",
			expectedOutput: "cat{bot{}str{abc}str{abc}str{abc}que{str{abc}}eot{\\z}}",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			re, err := parseRegexp(tc.input)
			require.NoError(t, err)
			parsed := EnsureRegexpAnchored(re)
			assert.Equal(t, tc.expectedOutput, dumpRegexp(parsed))
		})
	}
}

type testCase struct {
	name           string
	input          string
	expectedOutput string
}

// nolint
// only used for debugging
func pprintAst(ast *syntax.Regexp) {
	println(fmt.Sprintf("%+v", *ast))
	for i, s := range ast.Sub {
		println(fmt.Sprintf("%d>", i))
		pprintAst(s)
	}
}

// NB(prateek): adapted from https://golang.org/src/regexp/syntax/parse_test.go#L315
var opNames = []string{
	syntax.OpNoMatch:        "no",
	syntax.OpEmptyMatch:     "emp",
	syntax.OpLiteral:        "lit",
	syntax.OpCharClass:      "cc",
	syntax.OpAnyCharNotNL:   "dnl",
	syntax.OpAnyChar:        "dot",
	syntax.OpBeginLine:      "bol",
	syntax.OpEndLine:        "eol",
	syntax.OpBeginText:      "bot",
	syntax.OpEndText:        "eot",
	syntax.OpWordBoundary:   "wb",
	syntax.OpNoWordBoundary: "nwb",
	syntax.OpCapture:        "cap",
	syntax.OpStar:           "star",
	syntax.OpPlus:           "plus",
	syntax.OpQuest:          "que",
	syntax.OpRepeat:         "rep",
	syntax.OpConcat:         "cat",
	syntax.OpAlternate:      "alt",
}

// dumpRegexp writes an encoding of the syntax tree for the regexp re to b.
// It is used during testing to distinguish between parses that might print
// the same using re's String method.
func dumpRegexp(re *syntax.Regexp) string {
	var b strings.Builder
	dumpRegexpHelper(&b, re)
	return b.String()
}

func dumpRegexpHelper(b *strings.Builder, re *syntax.Regexp) {
	if int(re.Op) >= len(opNames) || opNames[re.Op] == "" {
		fmt.Fprintf(b, "op%d", re.Op)
	} else {
		switch re.Op {
		default:
			b.WriteString(opNames[re.Op])
		case syntax.OpStar, syntax.OpPlus, syntax.OpQuest, syntax.OpRepeat:
			if re.Flags&syntax.NonGreedy != 0 {
				b.WriteByte('n')
			}
			b.WriteString(opNames[re.Op])
		case syntax.OpLiteral:
			if len(re.Rune) > 1 {
				b.WriteString("str")
			} else {
				b.WriteString("lit")
			}
			if re.Flags&syntax.FoldCase != 0 {
				for _, r := range re.Rune {
					if unicode.SimpleFold(r) != r {
						b.WriteString("fold")
						break
					}
				}
			}
		}
	}
	b.WriteByte('{')
	switch re.Op {
	case syntax.OpEndText:
		if re.Flags&syntax.WasDollar == 0 {
			b.WriteString(`\z`)
		}
	case syntax.OpLiteral:
		for _, r := range re.Rune {
			b.WriteRune(r)
		}
	case syntax.OpConcat, syntax.OpAlternate:
		for _, sub := range re.Sub {
			dumpRegexpHelper(b, sub)
		}
	case syntax.OpStar, syntax.OpPlus, syntax.OpQuest:
		dumpRegexpHelper(b, re.Sub[0])
	case syntax.OpRepeat:
		fmt.Fprintf(b, "%d,%d ", re.Min, re.Max)
		dumpRegexpHelper(b, re.Sub[0])
	case syntax.OpCapture:
		if re.Name != "" {
			b.WriteString(re.Name)
			b.WriteByte(':')
		}
		dumpRegexpHelper(b, re.Sub[0])
	case syntax.OpCharClass:
		sep := ""
		for i := 0; i < len(re.Rune); i += 2 {
			b.WriteString(sep)
			sep = " "
			lo, hi := re.Rune[i], re.Rune[i+1]
			if lo == hi {
				fmt.Fprintf(b, "%#x", lo)
			} else {
				fmt.Fprintf(b, "%#x-%#x", lo, hi)
			}
		}
	}
	b.WriteByte('}')
}

func TestRegexpCache(t *testing.T) {
	scope := tally.NewTestScope("", nil)

	SetRegexpCacheOptions(RegexpCacheOptions{Size: 1, Scope: scope})
	defer SetRegexpCacheOptions(RegexpCacheOptions{Size: 0})

	_, err := CompileRegex([]byte("foo.*bar"))
	require.NoError(t, err)

	tallytest.AssertCounterValue(t, 1, scope.Snapshot(), "m3ninx.regexp.cache.miss", nil)

	_, err = CompileRegex([]byte("foo.*bar"))
	require.NoError(t, err)

	tallytest.AssertCounterValue(t, 1, scope.Snapshot(), "m3ninx.regexp.cache.hit", nil)
}
