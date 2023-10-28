package regexp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatchesEmptyValue(t *testing.T) {
	tests := []struct {
		given string
		want  bool
	}{
		{given: "", want: true},
		{given: "x", want: false},
		{given: ".*", want: true},
		{given: ".+", want: false},
		{given: "x*", want: true},
		{given: "x+", want: false},
		{given: "|", want: true},
		{given: "\\|", want: false},
		{given: "[|]", want: false},
		{given: "(|)", want: true},
		{given: "a|b", want: false},
		{given: "a||b", want: true},
		{given: "a|", want: true},
		{given: "|a", want: true},
		{given: "|a|", want: true},
		{given: "a|b|c", want: false},
		{given: "||a|||b||||c||||", want: true},
		{given: "()", want: true},
		{given: "()*", want: true},
		{given: "()+", want: true},
		{given: "(ab)", want: false},
		{given: "(ab)+", want: false},
		{given: "(a|b)", want: false},
		{given: "(a||b)", want: true},
		{given: "(a|)", want: true},
		{given: "(|a)", want: true},
		{given: "(\\|a)", want: false},
		{given: "([|])", want: false},
		{given: "([|]a)", want: false},
		{given: "([|a])", want: false},
		{given: ".*|a", want: true},
		{given: ".+|a", want: false},
		{given: ".*|.+", want: true},
		{given: ".*|.*", want: true},
		{given: ".+|.+", want: false},
		{given: "a(|)", want: false},
		{given: "a(|)b", want: false},
		{given: "(|)(|)", want: true},
		{given: "(|)a(|)", want: false},
		{given: "(|).*(|)", want: true},
		{given: "(|).+(|)", want: false},
		{given: "\\d*", want: true},
		{given: "\\d+", want: false},
		{given: "(\\d*|a)", want: true},
		{given: "(a|\\d*)", want: true},
		{given: "(|\\d*)", want: true},
		{given: "a(\\d*)", want: false},
		{given: "(\\d*)a", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.given, func(t *testing.T) {
			res, err := MatchesEmptyValue([]byte(tt.given))
			require.NoError(t, err)
			require.Equal(t, tt.want, res)
		})
	}
}
