// +build big
//
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
	"math/rand"
	"os"
	"regexp"
	"regexp/syntax"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestRegexpCompilationProperty(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 100000
	parameters.MaxSize = 40
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	properties.Property("Regexp matches same strings after modifications", prop.ForAll(
		func(x *inputCase) (bool, error) {
			compiled := compileRegexp(x.re, t)

			re, err := regexp.Compile(x.re)
			if err != nil {
				return false, fmt.Errorf("unable to compile re [%v]: %v", x.re, err)
			}
			input := []byte(x.str)

			originalMatch := re.Match(input)
			compiledMatch := compiled.Match(input)
			if originalMatch != compiledMatch {
				return false, fmt.Errorf("don't match %v %v %+v", originalMatch, compiled, x)
			}

			return true, nil
		},
		genInputCase(),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func compileRegexp(x string, t *testing.T) *regexp.Regexp {
	ast, err := parseRegexp(x)
	require.NoError(t, err)
	astp, err := ensureRegexpUnanchored(ast)
	require.NoError(t, err)
	ast2p, err := ensureRegexpAnchored(astp)
	require.NoError(t, err)
	re, err := regexp.Compile(ast2p.String())
	require.NoError(t, err)
	return re
}

func genInputCase() gopter.Gen {
	return genRegexp(unicode.ASCII_Hex_Digit).Map(
		func(reString string, params *gopter.GenParameters) *inputCase {
			val := gen.RegexMatch(reString)(params)
			strAny, ok := val.Retrieve()
			if !ok {
				return nil
			}
			return &inputCase{
				re:  reString,
				str: strAny.(string),
			}
		}).SuchThat(func(ic *inputCase) bool {
		return ic != nil
	})
}

type inputCase struct {
	re  string
	str string
}

const regexpFlags = syntax.Perl

func genRegexp(language *unicode.RangeTable) gopter.Gen {
	return genRegexpAst(language).Map(func(r *syntax.Regexp) string {
		return strings.Replace(r.Simplify().String(), "\\", "\\\\", -1)
	})
}

func genRegexpAst(language *unicode.RangeTable) gopter.Gen {
	return gen.OneGenOf(
		genRegexpNoOperands(language),
		genRegexpLiteral(language),
		genRegexpSingleOperands(language),
		genRegexpAnyOperands(language),
	)
}

func genRegexpNoOperands(l *unicode.RangeTable) gopter.Gen {
	return gen.OneConstOf(
		syntax.OpEmptyMatch,
		syntax.OpAnyChar,
		syntax.OpBeginText,
		syntax.OpEndText,
		syntax.OpWordBoundary,
		syntax.OpNoWordBoundary,
	).Map(func(op syntax.Op) *syntax.Regexp {
		return &syntax.Regexp{
			Op:    op,
			Flags: regexpFlags,
		}
	})
}

func genRegexpLiteral(language *unicode.RangeTable) gopter.Gen {
	return gen.UnicodeString(language).Map(func(s string) *syntax.Regexp {
		return &syntax.Regexp{
			Op:    syntax.OpLiteral,
			Flags: regexpFlags,
			Rune:  []rune(s),
		}
	})
}

func genRegexpSingleOperands(l *unicode.RangeTable) gopter.Gen {
	return gopter.CombineGens(
		gen.OneGenOf(genRegexpLiteral(l), genRegexpNoOperands(l)),
		gen.OneConstOf(
			syntax.OpCapture,
			syntax.OpStar,
			syntax.OpPlus,
			syntax.OpQuest,
			syntax.OpRepeat),
	).Map(func(vals []interface{}) *syntax.Regexp {
		return &syntax.Regexp{
			Op:    vals[1].(syntax.Op),
			Flags: regexpFlags,
			Sub:   []*syntax.Regexp{vals[0].(*syntax.Regexp)},
		}
	})
}

func genRegexpAnyOperands(l *unicode.RangeTable) gopter.Gen {
	return gopter.CombineGens(
		gen.SliceOf(
			gen.OneGenOf(
				genRegexpLiteral(l),
				genRegexpNoOperands(l),
				genRegexpSingleOperands(l),
			)),
		gen.OneConstOf(
			syntax.OpConcat,
			syntax.OpAlternate),
	).Map(func(vals []interface{}) *syntax.Regexp {
		return &syntax.Regexp{
			Op:    vals[1].(syntax.Op),
			Flags: regexpFlags,
			Sub:   vals[0].([]*syntax.Regexp),
		}
	})
}
