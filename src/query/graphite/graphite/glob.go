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

package graphite

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/m3db/m3/src/x/errors"
)

const (
	// ValidIdentifierRunes contains all the runes allowed in a graphite identifier
	ValidIdentifierRunes = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789" +
		"$-_'|<>%#/:~"
)

// GlobToRegexPattern converts a graphite-style glob into a regex pattern, with
// a boolean indicating if the glob is regexed or not
func GlobToRegexPattern(glob string) ([]byte, bool, error) {
	return globToRegexPattern(glob, GlobOptions{})
}

// ExtendedGlobToRegexPattern converts a graphite-style glob into a regex pattern
// with extended options
func ExtendedGlobToRegexPattern(glob string, opts GlobOptions) ([]byte, bool, error) {
	return globToRegexPattern(glob, opts)
}

// GlobOptions allows for matching everything
type GlobOptions struct {
	// AllowMatchAll allows for matching all text
	// including hierarchy separators with "**"
	AllowMatchAll bool `yaml:"allowMatchAll"`
}

type pattern struct {
	buff         bytes.Buffer
	eval         rune
	lastWriteLen int
}

func (p *pattern) String() string {
	return p.buff.String()
}

func (p *pattern) Evaluate(r rune) {
	p.eval = r
}

func (p *pattern) LastEvaluate() rune {
	return p.eval
}

func (p *pattern) WriteRune(r rune) {
	p.buff.WriteRune(r)
	p.lastWriteLen = 1
}

func (p *pattern) WriteString(str string) {
	p.buff.WriteString(str)
	p.lastWriteLen = len(str)
}

func (p *pattern) UnwriteLast() {
	p.buff.Truncate(p.buff.Len() - p.lastWriteLen)
	p.lastWriteLen = 0
}

func globToRegexPattern(glob string, opts GlobOptions) ([]byte, bool, error) {
	var (
		p            pattern
		escaping     bool
		regexed      bool
		matchAll     bool
		prevMatchAll bool
	)

	groupStartStack := []rune{rune(0)} // rune(0) indicates pattern is not in a group
	for i, r := range glob {
		// Evaluate if last was a matchAll statement and reset current matchAll.
		prevMatchAll = matchAll
		matchAll = false

		prevEval := p.LastEvaluate()
		p.Evaluate(r)

		if escaping {
			p.WriteRune(r)
			escaping = false
			continue
		}

		switch r {
		case '\\':
			escaping = true
			p.WriteRune('\\')
		case '.':
			// Check that previous rune was not the match all statement
			// since we need to skip adding a trailing dot to pattern if so.
			// It's meant to match zero or more segment separators.
			// e.g. "foo.**.bar.baz" glob should match:
			// - foo.term.bar.baz
			// - foo.term1.term2.bar.baz
			// - foo.bar.baz
			if !prevMatchAll {
				// Match hierarchy separator
				p.WriteString("\\.+")
				regexed = true
			}
		case '?':
			// Match anything except the hierarchy separator
			p.WriteString("[^\\.]")
			regexed = true
		case '*':
			if opts.AllowMatchAll && prevEval == '*' {
				p.UnwriteLast()
				p.WriteString(".*")
				regexed = true
				matchAll = true
			} else {
				// Match everything up to the next hierarchy separator
				p.WriteString("[^\\.]*")
				regexed = true
			}
		case '{':
			// Begin non-capturing group
			p.WriteString("(")
			groupStartStack = append(groupStartStack, r)
			regexed = true
		case '}':
			// End non-capturing group
			priorGroupStart := groupStartStack[len(groupStartStack)-1]
			if priorGroupStart != '{' {
				return nil, false, errors.NewInvalidParamsError(fmt.Errorf("invalid '}' at %d, no prior for '{' in %s", i, glob))
			}

			p.WriteRune(')')
			groupStartStack = groupStartStack[:len(groupStartStack)-1]
		case '[':
			// Begin character range
			p.WriteRune('[')
			groupStartStack = append(groupStartStack, r)
			regexed = true
		case ']':
			// End character range
			priorGroupStart := groupStartStack[len(groupStartStack)-1]
			if priorGroupStart != '[' {
				return nil, false, errors.NewInvalidParamsError(fmt.Errorf("invalid ']' at %d, no prior for '[' in %s", i, glob))
			}

			p.WriteRune(']')
			groupStartStack = groupStartStack[:len(groupStartStack)-1]
		case '<', '>', '\'', '$':
			p.WriteRune('\\')
			p.WriteRune(r)
		case ',':
			// Commas are part of the pattern if they appear in a group
			if groupStartStack[len(groupStartStack)-1] == '{' {
				p.WriteRune('|')
			} else {
				return nil, false, errors.NewInvalidParamsError(fmt.Errorf("invalid ',' outside of matching group at pos %d in %s", i, glob))
			}
		default:
			if !strings.ContainsRune(ValidIdentifierRunes, r) {
				return nil, false, errors.NewInvalidParamsError(fmt.Errorf("invalid character %c at pos %d in %s", r, i, glob))
			}
			p.WriteRune(r)
		}
	}

	if len(groupStartStack) > 1 {
		return nil, false, errors.NewInvalidParamsError(fmt.Errorf("unbalanced '%c' in %s", groupStartStack[len(groupStartStack)-1], glob))
	}

	return p.buff.Bytes(), regexed, nil
}
