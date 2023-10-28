// Copyright (c) 2023  Uber Technologies, Inc.
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

// Package regexp contains regexp processing related utilities.
package regexp

import (
	"regexp"
	"regexp/syntax"

	"github.com/m3db/m3/src/m3ninx/index"
)

// MatchesEmptyValue returns true if the given regexp would match an empty value.
func MatchesEmptyValue(expr []byte) (bool, error) {
	parsed, err := syntax.Parse(string(expr), syntax.Perl)
	if err != nil {
		return false, err //nolint:propagate_error
	}

	switch matchesEmptyValueAnalytically(parsed) {
	case yes:
		return true, nil
	case no:
		return false, nil
	default: // unknown - only now we resort to compilation and actual attempt to match the regexp
		return matchesEmptyValueEmpirically(parsed)
	}
}

func matchesEmptyValueAnalytically(r *syntax.Regexp) threeValuedLogic {
	switch r.Op {
	case syntax.OpEmptyMatch:
		return yes

	case syntax.OpLiteral:
		if len(r.Rune) == 0 {
			return yes
		}
		return no

	case syntax.OpCharClass:
		return no

	case syntax.OpStar:
		return yes

	case syntax.OpCapture, syntax.OpPlus:
		return matchesEmptyValueAnalytically(r.Sub[0])

	case syntax.OpConcat:
		var res = yes
		for _, s := range r.Sub {
			if m := matchesEmptyValueAnalytically(s); m == no {
				return no
			} else if m == unknown {
				res = unknown
			}
		}
		return res

	case syntax.OpAlternate:
		var res = no
		for _, s := range r.Sub {
			if m := matchesEmptyValueAnalytically(s); m == yes {
				return yes
			} else if m == unknown {
				res = unknown
			}
		}
		return res

	default:
		// If we even hit this case then we should fall back to
		// compiling and running the regexp against an empty string.
		return unknown
	}
}

// matchesEmptyValueEmpirically follows the logic of index.CompileRegex(expr).
func matchesEmptyValueEmpirically(r *syntax.Regexp) (bool, error) {
	unanchored, err := index.EnsureRegexpUnanchored(r)
	if err != nil {
		return false, err //nolint:propagate_error
	}

	anchored := index.EnsureRegexpAnchored(unanchored)

	return regexp.Match(anchored.String(), nil)
}

type threeValuedLogic uint8

const (
	no threeValuedLogic = iota
	yes
	unknown
)
