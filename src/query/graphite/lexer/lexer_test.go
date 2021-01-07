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

package lexer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type lexerTestData struct {
	input          string
	expectedTokens []Token
	reservedIdents map[string]TokenType
}

func TestLexer(t *testing.T) {
	lexerTestInput := []lexerTestData{
		{"call09", []Token{{Identifier, "call09"}}, nil},
		{"sortByName(foo.bar.zed)", []Token{
			{Identifier, "sortByName"},
			{LParenthesis, "("},
			{Pattern, "foo.bar.zed"},
			{RParenthesis, ")"}}, nil},
		{"foo.'bar<1001>'.baz",
			[]Token{{Pattern, "foo.'bar<1001>'.baz"}}, nil},
		{"a.{b,c,d}.node[0-2].qux.*",
			[]Token{{Pattern, "a.{b,c,d}.node[0-2].qux.*"}}, nil},
		{"qaz{0[3-9],1[0-9],20}",
			[]Token{{Pattern, "qaz{0[3-9],1[0-9],20}"}}, nil},
		{"qaz{0[3-9],1[0-9],20}.bar",
			[]Token{{Pattern, "qaz{0[3-9],1[0-9],20}.bar"}}, nil},
		{"stats.foo.counts.bar.baz.status_code.?XX",
			[]Token{{Pattern, "stats.foo.counts.bar.baz.status_code.?XX"}}, nil},
		{"456.03", []Token{{Number, "456.03"}}, nil},
		{"foo:bar", []Token{
			{Identifier, "foo:bar"}}, nil},
		{"foo:bar  baz:q*x", []Token{
			{Identifier, "foo:bar"},
			{Pattern, "baz:q*x"}}, nil},
		{"!service:dispatch.*", []Token{
			{NotOperator, "!"},
			{Pattern, "service:dispatch.*"}}, nil},
		{"\"Whatever man\"", []Token{{String, "Whatever man"}}, nil}, // double quoted string
		// no need to escape single quotes within double quoted string
		{"\"Whatever 'man'\"", []Token{{String, "Whatever 'man'"}}, nil},
		// escape double quotes within double quoted string
		{`"Whatever \"man\""`, []Token{{String, "Whatever \"man\""}}, nil},
		{"'Whatever man'", []Token{{String, "Whatever man"}}, nil}, // single quoted string
		// no need to escape double quote within single quoted strings (start boundary), but may
		// do it if so desired (end boundary)
		{`'Whatever "man\"'`, []Token{{String, "Whatever \"man\\\""}}, nil},
		{" call09(a.{b,c,d}.node[0-2].qux.*, a{e,g}, 4546.abc, 45ahj, " +
			`"Hello there \"Good \ Sir\" ", +20, 39540.459,-349845,.393) `,
			[]Token{
				{Identifier, "call09"},
				{LParenthesis, "("},
				{Pattern, "a.{b,c,d}.node[0-2].qux.*"},
				{Comma, ","},
				{Pattern, "a{e,g}"},
				{Comma, ","},
				{Pattern, "4546.abc"},
				{Comma, ","},
				{Pattern, "45ahj"},
				{Comma, ","},
				{String, "Hello there \"Good \\ Sir\" "},
				{Comma, ","},
				{Number, "+20"},
				{Comma, ","},
				{Number, "39540.459"},
				{Comma, ","},
				{Number, "-349845"},
				{Comma, ","},
				{Number, ".393"},
				{RParenthesis, ")"}}, nil},
		{`aliasSub(stats.foo.timers.scream.scream.views.quz.end_to_end_latency.p95, ` +
			`'stats.(.*).timers.scream.scream.views.quz.(.*)', '\1.\2')`,
			[]Token{
				{Identifier, "aliasSub"},
				{LParenthesis, "("},
				{Pattern, "stats.foo.timers.scream.scream.views.quz.end_to_end_latency.p95"},
				{Comma, ","},
				{String, "stats.(.*).timers.scream.scream.views.quz.(.*)"},
				{Comma, ","},
				{String, `\1.\2`},
				{RParenthesis, ")"}}, nil},
		{"sumSeries(stats.with:colon)",
			[]Token{
				{Identifier, "sumSeries"},
				{LParenthesis, "("},
				{Pattern, "stats.with:colon"},
				{RParenthesis, ")"},
			},
			nil},
		{"sumSeries(stats.with:colon.extended.pattern.*)",
			[]Token{
				{Identifier, "sumSeries"},
				{LParenthesis, "("},
				{Pattern, "stats.with:colon.extended.pattern.*"},
				{RParenthesis, ")"},
			},
			nil},
	}

	for _, test := range lexerTestInput {
		lex, tokens := NewLexer(test.input, test.reservedIdents, Options{})
		go lex.Run()

		i := 0
		for token := range tokens {
			require.True(t, i < len(test.expectedTokens),
				"received more tokens than expected for %s", test.input)
			assert.Equal(t, &test.expectedTokens[i], token, "incorrect token %d for %s",
				i, test.input)
			i++
		}

		assert.True(t, lex.eof(), "did not reach eof for %s", test.input)
	}
}

func TestLexerErrors(t *testing.T) {
	badLines := [][]string{
		{"\"this is \\\"unterminated",
			"reached end of input while processing string \"this is \\\"unterminated"},
		{"ajs.djd]", // group closed without open
			"encountered unbalanced end of group ] in pattern ajs.djd]"},
		{"{dfklf]", // mismatch between open and close of group
			"encountered unbalanced end of group ] in pattern {dfklf]"},
		{"{[ajs.djd}]", // flipped close groups
			"encountered unbalanced end of group } in pattern {[ajs.djd}"},
		{"{unclosed", "end of pattern {unclosed reached while still in group {"}, // unclosed group
		{"+a", "expected one of 0123456789, found a"},                            // plus with no number
		{"-b", "expected one of 0123456789, found b"},                            // minus with no number
		{".c", "expected one of 0123456789, found c"},                            // digit with no number
		{"^", "unexpected character ^"},                                          // unknown symbol
	}

	for _, badLine := range badLines {
		l, tokens := NewLexer(badLine[0], nil, Options{})
		go l.Run()

		expected := &Token{Error, badLine[1]}
		actual := <-tokens
		assert.Equal(t, expected, actual, "%s did not result in error", badLine[0])

		// Should have closed the channel after the error
		actual = <-tokens
		assert.Nil(t, actual)
	}
}
