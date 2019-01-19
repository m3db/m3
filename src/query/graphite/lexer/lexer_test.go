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
		{"cities.'china<1001>'.demarsk",
			[]Token{{Pattern, "cities.'china<1001>'.demarsk"}}, nil},
		{"a.{b,c,d}.node[0-2].carbon-daemons.*",
			[]Token{{Pattern, "a.{b,c,d}.node[0-2].carbon-daemons.*"}}, nil},
		{"optic{0[3-9],1[0-9],20}",
			[]Token{{Pattern, "optic{0[3-9],1[0-9],20}"}}, nil},
		{"optic{0[3-9],1[0-9],20}.production",
			[]Token{{Pattern, "optic{0[3-9],1[0-9],20}.production"}}, nil},
		{"stats.sjc1.counts.haproxy.products.status_code.?XX",
			[]Token{{Pattern, "stats.sjc1.counts.haproxy.products.status_code.?XX"}}, nil},
		{"456.03", []Token{{Number, "456.03"}}, nil},
		{"foo:bar", []Token{
			{Identifier, "foo"},
			{Colon, ":"},
			{Identifier, "bar"}}, nil},
		{"foo:bar  baz : q*x", []Token{
			{Identifier, "foo"},
			{Colon, ":"},
			{Identifier, "bar"},
			{Identifier, "baz"},
			{Colon, ":"},
			{Pattern, "q*x"}}, nil},
		{"!service:dispatch.*", []Token{
			{NotOperator, "!"},
			{Identifier, "service"},
			{Colon, ":"},
			{Pattern, "dispatch.*"}}, nil},
		{"\"Whatever man\"", []Token{{String, "Whatever man"}}, nil}, // double quoted string
		// no need to escape single quotes within double quoted string
		{"\"Whatever 'man'\"", []Token{{String, "Whatever 'man'"}}, nil},
		// escape double quotes within double quoted string
		{"\"Whatever \\\"man\\\"\"", []Token{{String, "Whatever \"man\""}}, nil},
		{"'Whatever man'", []Token{{String, "Whatever man"}}, nil}, // single quoted string
		// no need to escape double quote within single quoted strings (start boundary), but may
		// do it if so desired (end boundary)
		{"'Whatever \"man\\\"'", []Token{{String, "Whatever \"man\""}}, nil},
		{" call09(a.{b,c,d}.node[0-2].carbon-daemons.*, a{e,g}, 4546.abc, 45ahj, " +
			"\"Hello there \\\"Good \\\\ Sir\\\" \", +20, 39540.459,-349845,.393) ",
			[]Token{
				{Identifier, "call09"},
				{LParenthesis, "("},
				{Pattern, "a.{b,c,d}.node[0-2].carbon-daemons.*"},
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
		{`aliasSub(stats.sjc1.timers.scream.scream.views.record_trip.end_to_end_latency.p95, ` +
			`'stats.(.*).timers.scream.scream.views.record_trip.(.*)', '\1.\2')`,
			[]Token{
				{Identifier, "aliasSub"},
				{LParenthesis, "("},
				{Pattern, "stats.sjc1.timers.scream.scream.views.record_trip.end_to_end_latency.p95"},
				{Comma, ","},
				{String, "stats.(.*).timers.scream.scream.views.record_trip.(.*)"},
				{Comma, ","},
				{String, `\1.\2`},
				{RParenthesis, ")"}}, nil},
	}

	for _, test := range lexerTestInput {
		lex, tokens := NewLexer(test.input, test.reservedIdents)
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
		l, tokens := NewLexer(badLine[0], nil)
		go l.Run()

		expected := &Token{Error, badLine[1]}
		actual := <-tokens
		assert.Equal(t, expected, actual, "%s did not result in error", badLine[0])

		// Should have closed the channel after the error
		actual = <-tokens
		assert.Nil(t, actual)
	}
}
