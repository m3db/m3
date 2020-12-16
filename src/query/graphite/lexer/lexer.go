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
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/m3db/m3/src/query/graphite/graphite"
)

// TokenType defines the type of identifier recognized by the Lexer.
type TokenType int

const (
	// Error is what you get when the lexer fails to grok the input.
	Error TokenType = iota
	// Identifier is a symbol confining to C-style variable naming rules.
	Identifier
	// Pattern is a regex-ish pattern, accepts the following special chars: [{.*}].
	Pattern
	// Number is a numeral, including floats.
	Number
	// String is set of characters wrapped by double quotes.
	String
	// LParenthesis is the left parenthesis "(".
	LParenthesis
	// RParenthesis is the right parenthesis ")".
	RParenthesis
	// NotOperator is the exclamation sign - "!" symbol.
	NotOperator
	// Comma is a punctuation mark.
	Comma
	// Equal is the "=" symbol.
	Equal

	// True is Boolean true.
	True
	// False is Boolean false.
	False
)

func (tt TokenType) String() string {
	switch tt {
	case Error:
		return "Error"
	case Identifier:
		return "Identifier"
	case Pattern:
		return "Pattern"
	case Number:
		return "Number"
	case String:
		return "String"
	case LParenthesis:
		return "LParenthesis"
	case RParenthesis:
		return "RParenthesis"
	case NotOperator:
		return "NotOperator"
	case Comma:
		return "Comma"
	case Equal:
		return "Equal"
	case True:
		return "True"
	case False:
		return "False"
	}
	return fmt.Sprintf("UnknownToken(%d)", int(tt))
}

var symbols = map[rune]TokenType{
	'(': LParenthesis,
	')': RParenthesis,
	'!': NotOperator,
	',': Comma,
	'=': Equal,
}

// Token is a token, doh!
type Token struct {
	tokenType TokenType
	value     string
}

// MustMakeToken is a test function for creating a Token.MustMakeToken.
func MustMakeToken(value string) *Token { return &Token{value: value} }

// TokenType returns the type of token consumed.
func (t Token) TokenType() TokenType {
	return t.tokenType
}

// Value returns the string representation of the token as needed.
func (t Token) Value() string {
	return t.value
}

const (
	uppercaseLetters     = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	lowercaseLetters     = "abcdefghijklmnopqrstuvwxyz"
	digits               = "0123456789"
	exponentRunes        = "eE"
	identifierStartRunes = uppercaseLetters + lowercaseLetters + "_" + "-" + "$" + ":"
	identifierRunes      = identifierStartRunes + digits
	signs                = "+-"
)

// Lexer breaks an input stream into a group of lexical elements.
type Lexer struct {
	tokens              chan *Token
	s                   string
	start               int
	pos                 int
	width               int
	reservedIdentifiers map[string]TokenType
	opts                Options
}

const (
	eof rune = 0
)

// Options allows for specifying lexer options.
type Options struct {
	EscapeAllNotOnlyQuotes bool
}

// NewLexer returns a lexer and an output channel for tokens.
func NewLexer(s string, reservedIdentifiers map[string]TokenType, opts Options) (*Lexer, chan *Token) {
	tokens := make(chan *Token)
	return &Lexer{
		s:                   s,
		tokens:              tokens,
		reservedIdentifiers: reservedIdentifiers,
		opts:                opts,
	}, tokens
}

// Run consumes the input to produce a token stream.
func (l *Lexer) Run() {
	for l.lex() {
	}
	close(l.tokens)
}

func (l *Lexer) lex() bool {
	l.skipWhitespace()

	r := l.next()
	if r == eof {
		return false
	}

	if r == '"' || r == '\'' {
		return l.quotedString(r)
	}

	if r == '+' || r == '-' {
		return l.positiveOrNegativeNumber()
	}

	if r == '.' {
		return l.fractionalOnlyNumber()
	}

	if strings.ContainsRune(digits, r) {
		return l.numberOrPattern()
	}

	if strings.ContainsRune(identifierStartRunes, r) {
		return l.identifierOrPattern()
	}

	if strings.ContainsRune("{[*.", r) {
		l.backup()
		return l.pattern()
	}

	sym, ok := symbols[r]
	if !ok {
		return l.errorf("unexpected character %c", r)
	}

	l.emit(sym)
	return true
}

func (l *Lexer) eof() bool {
	l.skipWhitespace()
	return l.pos >= len(l.s)
}

func (l *Lexer) positiveOrNegativeNumber() bool {
	if !l.acceptRun(digits) {
		return l.unexpected(digits)
	}

	if l.accept(".") {
		return l.fractionalPart()
	}

	l.emit(Number)
	return true
}

func (l *Lexer) fractionalOnlyNumber() bool {
	if !l.acceptRun(digits) {
		return l.unexpected(digits)
	}
	if l.accept(exponentRunes) {
		return l.exponentPart()
	}
	l.emit(Number)
	return true
}

func (l *Lexer) fractionalPart() bool {
	l.acceptRun(digits)
	l.emit(Number)
	return true
}

func (l *Lexer) exponentPart() bool {
	l.accept(signs)
	if !l.acceptRun(digits) {
		return l.unexpected(digits)
	}
	l.emit(Number)
	return true
}

func (l *Lexer) numberOrPattern() bool {
	l.acceptRun(digits)
	if l.accept(".") {
		return l.fractionalPartOrPattern()
	}

	r := l.next()
	if r != eof {
		l.backup()
	}
	if l.accept(exponentRunes) {
		return l.exponentPart()
	}
	if strings.ContainsRune("{[*-"+identifierStartRunes, r) {
		return l.pattern()
	}

	l.emit(Number)
	return true
}

func (l *Lexer) fractionalPartOrPattern() bool {
	l.acceptRun(digits)

	r := l.next()
	if r != eof {
		l.backup()
	}
	if l.accept(exponentRunes) {
		return l.exponentPart()
	}
	if strings.ContainsRune("{[*-."+identifierStartRunes, r) {
		return l.pattern()
	}

	l.emit(Number)
	return true
}

func (l *Lexer) identifierOrPattern() bool {
	l.acceptRun(identifierRunes)

	r := l.next()
	if r != eof {
		l.backup()
	}
	if strings.ContainsRune("{[*.-", r) {
		return l.pattern()
	}

	// Check if identifier is one of the reserved identifiers.
	for text, identifier := range l.reservedIdentifiers {
		if strings.ToLower(l.currentVal()) == text {
			l.emit(identifier)
			return true
		}
	}

	l.emit(Identifier)
	return true
}

// NB(jayp): initialized by init().
var groupingEndsToStarts = map[rune]rune{}

var groupingStartsToEnds = map[rune]rune{
	'{': '}',
	'[': ']',
}

func (l *Lexer) pattern() bool {
	// rune(0) indicates pattern is not in a group.
	groupStartStack := []rune{rune(0)}
	for {
		r := l.next()

		// Start of a group.
		if _, ok := groupingStartsToEnds[r]; ok {
			// Start another group.
			groupStartStack = append(groupStartStack, r)
			continue
		}

		// End of a group.
		if groupStart, ok := groupingEndsToStarts[r]; ok {
			// Unwind group.
			if groupStart != groupStartStack[len(groupStartStack)-1] {
				return l.errorf("encountered unbalanced end of group %c in pattern %s",
					r, l.currentVal())
			}
			groupStartStack = groupStartStack[:len(groupStartStack)-1]
			continue
		}

		if strings.ContainsRune(graphite.ValidIdentifierRunes+".?*", r) {
			continue
		}

		// Commas are part of the pattern if they appear in a group
		if r == ',' && groupStartStack[len(groupStartStack)-1] != 0 {
			continue
		}

		// Everything else is the end of the pattern.
		if groupStartStack[len(groupStartStack)-1] != 0 {
			return l.errorf("end of pattern %s reached while still in group %c",
				l.currentVal(), groupStartStack[len(groupStartStack)-1])
		}

		if r != eof {
			l.backup()
		}
		l.emit(Pattern)
		return true
	}
}

func (l *Lexer) quotedString(quoteMark rune) bool {
	var s []rune
	escaped := false
	for {
		r := l.next()
		if r == eof {
			return l.errorf("reached end of input while processing string %s", l.currentVal())
		}

		if !escaped && r == quoteMark {
			l.emitToken(String, string(s))
			l.consumeVal()
			return true
		}

		if !escaped && r == '\\' {
			// TODO: Want to omit this from the output.
			escaped = true
			continue
		}

		// By default we only need escaping for quotes and treat
		// backslashes as regular backslashes (i.e. for use in regexp
		// with aliasSub, etc) and as such restore backslash as long not
		// escaping a quote.
		restoreBackslash := escaped && r != quoteMark
		if l.opts.EscapeAllNotOnlyQuotes {
			// If escaping all characters not just quotes then only restore
			// backslash if using it for regex group replacement (i.e. "\1").
			restoreBackslash = escaped && strings.ContainsRune(digits, r)
		}
		if restoreBackslash {
			// If backslash not being used to escape quote then keep it.
			s = append(s, '\\')
		}

		s = append(s, r)
		escaped = false
	}
}

func (l *Lexer) unexpected(expected string) bool {
	r := l.next()
	l.backup()
	return l.errorf("expected one of %s, found %c", expected, r)
}

func (l *Lexer) skipWhitespace() {
	l.acceptRun(" \t\r\n")
	l.ignore()
}

func (l *Lexer) next() (r rune) {
	if l.pos >= len(l.s) {
		l.width = 0
		return eof
	}

	r, l.width = utf8.DecodeRuneInString(l.s[l.pos:])
	l.pos += l.width
	return r
}

func (l *Lexer) ignore() {
	l.start = l.pos
}

func (l *Lexer) backup() {
	l.pos--
}

func (l *Lexer) accept(valid string) bool {
	r := l.next()
	if r != eof && strings.ContainsRune(valid, r) {
		return true
	}

	if r != eof {
		l.backup()
	}
	return false
}

func (l *Lexer) acceptRun(valid string) bool {
	matched := false

	r := l.next()
	for strings.ContainsRune(valid, r) && r != eof {
		matched = true
		r = l.next()
	}

	if r != eof {
		l.backup()
	}

	return matched
}

func (l *Lexer) currentVal() string {
	return l.s[l.start:l.pos]
}

func (l *Lexer) consumeVal() string {
	s := l.currentVal()
	l.start = l.pos
	return s
}

func (l *Lexer) emit(tt TokenType) {
	l.emitToken(tt, l.consumeVal())
}

func (l *Lexer) emitToken(tt TokenType, val string) {
	l.tokens <- &Token{
		tokenType: tt,
		value:     val,
	}
}

func (l *Lexer) errorf(msg string, args ...interface{}) bool {
	l.tokens <- &Token{
		tokenType: Error,
		value:     fmt.Sprintf(msg, args...),
	}
	return false
}

func init() {
	for start, end := range groupingStartsToEnds {
		groupingEndsToStarts[end] = start
	}
}
