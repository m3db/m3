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

package m3ql

//go:generate peg -inline -switch src/query/parser/m3ql/grammar.peg

import (
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
)

const endSymbol rune = 1114112

/* The rule types inferred from the grammar are below. */
type pegRule uint8

const (
	ruleUnknown pegRule = iota
	ruleGrammar
	ruleMacroDef
	rulePipeline
	ruleExpression
	ruleFunctionCall
	ruleArgument
	ruleKeywordSpecifier
	ruleNesting
	ruleSpacing
	ruleSpace
	ruleEOL
	ruleComment
	ruleCommentStart
	ruleIdentifier
	ruleIdentifierStart
	ruleIdentifierChars
	ruleOperator
	ruleOperatorSymbols
	ruleBoolean
	ruleTrue
	ruleFalse
	ruleNumber
	ruleIntegralNumber
	ruleFloatingNumber
	ruleMinus
	ruleStringLiteral
	ruleQuoteChar
	rulePattern
	rulePatternChars
	ruleGlobSymbols
	ruleSemicolon
	ruleEquals
	rulePipe
	ruleLParenthesis
	ruleRParenthesis
	ruleColon
	ruleEOF
	ruleAction0
	ruleAction1
	ruleAction2
	ruleAction3
	ruleAction4
	ruleAction5
	ruleAction6
	ruleAction7
	ruleAction8
	ruleAction9
	rulePegText
)

var rul3s = [...]string{
	"Unknown",
	"Grammar",
	"MacroDef",
	"Pipeline",
	"Expression",
	"FunctionCall",
	"Argument",
	"KeywordSpecifier",
	"Nesting",
	"Spacing",
	"Space",
	"EOL",
	"Comment",
	"CommentStart",
	"Identifier",
	"IdentifierStart",
	"IdentifierChars",
	"Operator",
	"OperatorSymbols",
	"Boolean",
	"True",
	"False",
	"Number",
	"IntegralNumber",
	"FloatingNumber",
	"Minus",
	"StringLiteral",
	"QuoteChar",
	"Pattern",
	"PatternChars",
	"GlobSymbols",
	"Semicolon",
	"Equals",
	"Pipe",
	"LParenthesis",
	"RParenthesis",
	"Colon",
	"EOF",
	"Action0",
	"Action1",
	"Action2",
	"Action3",
	"Action4",
	"Action5",
	"Action6",
	"Action7",
	"Action8",
	"Action9",
	"PegText",
}

type token32 struct {
	pegRule
	begin, end uint32
}

func (t *token32) String() string {
	return fmt.Sprintf("\x1B[34m%v\x1B[m %v %v", rul3s[t.pegRule], t.begin, t.end)
}

type node32 struct {
	token32
	up, next *node32
}

func (node *node32) print(w io.Writer, pretty bool, buffer string) {
	var print func(node *node32, depth int)
	print = func(node *node32, depth int) {
		for node != nil {
			for c := 0; c < depth; c++ {
				fmt.Printf(" ")
			}
			rule := rul3s[node.pegRule]
			quote := strconv.Quote(string(([]rune(buffer)[node.begin:node.end])))
			if !pretty {
				fmt.Fprintf(w, "%v %v\n", rule, quote)
			} else {
				fmt.Fprintf(w, "\x1B[34m%v\x1B[m %v\n", rule, quote)
			}
			if node.up != nil {
				print(node.up, depth+1)
			}
			node = node.next
		}
	}
	print(node, 0)
}

func (node *node32) Print(w io.Writer, buffer string) {
	node.print(w, false, buffer)
}

func (node *node32) PrettyPrint(w io.Writer, buffer string) {
	node.print(w, true, buffer)
}

type tokens32 struct {
	tree []token32
}

func (t *tokens32) Trim(length uint32) {
	t.tree = t.tree[:length]
}

func (t *tokens32) Print() {
	for _, token := range t.tree {
		fmt.Println(token.String())
	}
}

func (t *tokens32) AST() *node32 {
	type element struct {
		node *node32
		down *element
	}
	tokens := t.Tokens()
	var stack *element
	for _, token := range tokens {
		if token.begin == token.end {
			continue
		}
		node := &node32{token32: token}
		for stack != nil && stack.node.begin >= token.begin && stack.node.end <= token.end {
			stack.node.next = node.up
			node.up = stack.node
			stack = stack.down
		}
		stack = &element{node: node, down: stack}
	}
	if stack != nil {
		return stack.node
	}
	return nil
}

func (t *tokens32) PrintSyntaxTree(buffer string) {
	t.AST().Print(os.Stdout, buffer)
}

func (t *tokens32) WriteSyntaxTree(w io.Writer, buffer string) {
	t.AST().Print(w, buffer)
}

func (t *tokens32) PrettyPrintSyntaxTree(buffer string) {
	t.AST().PrettyPrint(os.Stdout, buffer)
}

func (t *tokens32) Add(rule pegRule, begin, end, index uint32) {
	if tree := t.tree; int(index) >= len(tree) {
		expanded := make([]token32, 2*len(tree))
		copy(expanded, tree)
		t.tree = expanded
	}
	t.tree[index] = token32{
		pegRule: rule,
		begin:   begin,
		end:     end,
	}
}

func (t *tokens32) Tokens() []token32 {
	return t.tree
}

type m3ql struct {
	scriptBuilder

	Buffer string
	buffer []rune
	rules  [49]func() bool
	parse  func(rule ...int) error
	reset  func()
	Pretty bool
	tokens32
}

func (p *m3ql) Parse(rule ...int) error {
	return p.parse(rule...)
}

func (p *m3ql) Reset() {
	p.reset()
}

type textPosition struct {
	line, symbol int
}

type textPositionMap map[int]textPosition

func translatePositions(buffer []rune, positions []int) textPositionMap {
	length, translations, j, line, symbol := len(positions), make(textPositionMap, len(positions)), 0, 1, 0
	sort.Ints(positions)

search:
	for i, c := range buffer {
		if c == '\n' {
			line, symbol = line+1, 0
		} else {
			symbol++
		}
		if i == positions[j] {
			translations[positions[j]] = textPosition{line, symbol}
			for j++; j < length; j++ {
				if i != positions[j] {
					continue search
				}
			}
			break search
		}
	}

	return translations
}

type parseError struct {
	p   *m3ql
	max token32
}

func (e *parseError) Error() string {
	tokens, error := []token32{e.max}, "\n"
	positions, p := make([]int, 2*len(tokens)), 0
	for _, token := range tokens {
		positions[p], p = int(token.begin), p+1
		positions[p], p = int(token.end), p+1
	}
	translations := translatePositions(e.p.buffer, positions)
	format := "parse error near %v (line %v symbol %v - line %v symbol %v):\n%v\n"
	if e.p.Pretty {
		format = "parse error near \x1B[34m%v\x1B[m (line %v symbol %v - line %v symbol %v):\n%v\n"
	}
	for _, token := range tokens {
		begin, end := int(token.begin), int(token.end)
		error += fmt.Sprintf(format,
			rul3s[token.pegRule],
			translations[begin].line, translations[begin].symbol,
			translations[end].line, translations[end].symbol,
			strconv.Quote(string(e.p.buffer[begin:end])))
	}

	return error
}

func (p *m3ql) PrintSyntaxTree() {
	if p.Pretty {
		p.tokens32.PrettyPrintSyntaxTree(p.Buffer)
	} else {
		p.tokens32.PrintSyntaxTree(p.Buffer)
	}
}

func (p *m3ql) WriteSyntaxTree(w io.Writer) {
	p.tokens32.WriteSyntaxTree(w, p.Buffer)
}

func (p *m3ql) Execute() {
	buffer, _buffer, text, begin, end := p.Buffer, p.buffer, "", 0, 0
	for _, token := range p.Tokens() {
		switch token.pegRule {

		case rulePegText:
			begin, end = int(token.begin), int(token.end)
			text = string(_buffer[begin:end])

		case ruleAction0:
			p.newMacro(text)
		case ruleAction1:
			p.newPipeline()
		case ruleAction2:
			p.endPipeline()
		case ruleAction3:
			p.newExpression(text)
		case ruleAction4:
			p.endExpression()
		case ruleAction5:
			p.newBooleanArgument(text)
		case ruleAction6:
			p.newNumericArgument(text)
		case ruleAction7:
			p.newPatternArgument(text)
		case ruleAction8:
			p.newStringLiteralArgument(text)
		case ruleAction9:
			p.newKeywordArgument(text)

		}
	}
	_, _, _, _, _ = buffer, _buffer, text, begin, end
}

func (p *m3ql) Init() {
	var (
		max                  token32
		position, tokenIndex uint32
		buffer               []rune
	)
	p.reset = func() {
		max = token32{}
		position, tokenIndex = 0, 0

		p.buffer = []rune(p.Buffer)
		if len(p.buffer) == 0 || p.buffer[len(p.buffer)-1] != endSymbol {
			p.buffer = append(p.buffer, endSymbol)
		}
		buffer = p.buffer
	}
	p.reset()

	_rules := p.rules
	tree := tokens32{tree: make([]token32, math.MaxInt16)}
	p.parse = func(rule ...int) error {
		r := 1
		if len(rule) > 0 {
			r = rule[0]
		}
		matches := p.rules[r]()
		p.tokens32 = tree
		if matches {
			p.Trim(tokenIndex)
			return nil
		}
		return &parseError{p, max}
	}

	add := func(rule pegRule, begin uint32) {
		tree.Add(rule, begin, position, tokenIndex)
		tokenIndex++
		if begin != position && position > max.end {
			max = token32{rule, begin, position}
		}
	}

	matchDot := func() bool {
		if buffer[position] != endSymbol {
			position++
			return true
		}
		return false
	}

	/*matchChar := func(c byte) bool {
		if buffer[position] == c {
			position++
			return true
		}
		return false
	}*/

	/*matchRange := func(lower byte, upper byte) bool {
		if c := buffer[position]; c >= lower && c <= upper {
			position++
			return true
		}
		return false
	}*/

	_rules = [...]func() bool{
		nil,
		/* 0 Grammar <- <(Spacing (MacroDef Semicolon)* Pipeline EOF)> */
		func() bool {
			position0, tokenIndex0 := position, tokenIndex
			{
				position1 := position
				if !_rules[ruleSpacing]() {
					goto l0
				}
			l2:
				{
					position3, tokenIndex3 := position, tokenIndex
					{
						position4 := position
						if !_rules[ruleIdentifier]() {
							goto l3
						}
						{
							add(ruleAction0, position)
						}
						{
							position6 := position
							if buffer[position] != rune('=') {
								goto l3
							}
							position++
							if !_rules[ruleSpacing]() {
								goto l3
							}
							add(ruleEquals, position6)
						}
						if !_rules[rulePipeline]() {
							goto l3
						}
						add(ruleMacroDef, position4)
					}
					{
						position7 := position
						if buffer[position] != rune(';') {
							goto l3
						}
						position++
						if !_rules[ruleSpacing]() {
							goto l3
						}
						add(ruleSemicolon, position7)
					}
					goto l2
				l3:
					position, tokenIndex = position3, tokenIndex3
				}
				if !_rules[rulePipeline]() {
					goto l0
				}
				{
					position8 := position
					{
						position9, tokenIndex9 := position, tokenIndex
						if !matchDot() {
							goto l9
						}
						goto l0
					l9:
						position, tokenIndex = position9, tokenIndex9
					}
					add(ruleEOF, position8)
				}
				add(ruleGrammar, position1)
			}
			return true
		l0:
			position, tokenIndex = position0, tokenIndex0
			return false
		},
		/* 1 MacroDef <- <(Identifier Action0 Equals Pipeline)> */
		nil,
		/* 2 Pipeline <- <(Action1 Expression (Pipe Expression)* Action2)> */
		func() bool {
			position11, tokenIndex11 := position, tokenIndex
			{
				position12 := position
				{
					add(ruleAction1, position)
				}
				if !_rules[ruleExpression]() {
					goto l11
				}
			l14:
				{
					position15, tokenIndex15 := position, tokenIndex
					{
						position16 := position
						if buffer[position] != rune('|') {
							goto l15
						}
						position++
						if !_rules[ruleSpacing]() {
							goto l15
						}
						add(rulePipe, position16)
					}
					if !_rules[ruleExpression]() {
						goto l15
					}
					goto l14
				l15:
					position, tokenIndex = position15, tokenIndex15
				}
				{
					add(ruleAction2, position)
				}
				add(rulePipeline, position12)
			}
			return true
		l11:
			position, tokenIndex = position11, tokenIndex11
			return false
		},
		/* 3 Expression <- <(FunctionCall / Nesting)> */
		func() bool {
			position18, tokenIndex18 := position, tokenIndex
			{
				position19 := position
				{
					position20, tokenIndex20 := position, tokenIndex
					{
						position22 := position
						{
							position23, tokenIndex23 := position, tokenIndex
							if !_rules[ruleIdentifier]() {
								goto l24
							}
							goto l23
						l24:
							position, tokenIndex = position23, tokenIndex23
							{
								position25 := position
								{
									position26 := position
									{
										position27 := position
										{
											position28, tokenIndex28 := position, tokenIndex
											if buffer[position] != rune('<') {
												goto l29
											}
											position++
											if buffer[position] != rune('=') {
												goto l29
											}
											position++
											goto l28
										l29:
											position, tokenIndex = position28, tokenIndex28
											if buffer[position] != rune('>') {
												goto l30
											}
											position++
											if buffer[position] != rune('=') {
												goto l30
											}
											position++
											goto l28
										l30:
											position, tokenIndex = position28, tokenIndex28
											{
												switch buffer[position] {
												case '>':
													if buffer[position] != rune('>') {
														goto l21
													}
													position++
													break
												case '!':
													if buffer[position] != rune('!') {
														goto l21
													}
													position++
													if buffer[position] != rune('=') {
														goto l21
													}
													position++
													break
												case '=':
													if buffer[position] != rune('=') {
														goto l21
													}
													position++
													if buffer[position] != rune('=') {
														goto l21
													}
													position++
													break
												default:
													if buffer[position] != rune('<') {
														goto l21
													}
													position++
													break
												}
											}

										}
									l28:
										add(ruleOperatorSymbols, position27)
									}
									add(rulePegText, position26)
								}
								if !_rules[ruleSpacing]() {
									goto l21
								}
								add(ruleOperator, position25)
							}
						}
					l23:
						{
							add(ruleAction3, position)
						}
					l33:
						{
							position34, tokenIndex34 := position, tokenIndex
							{
								position35 := position
								{
									position36, tokenIndex36 := position, tokenIndex
									{
										position38 := position
										if !_rules[ruleIdentifier]() {
											goto l36
										}
										{
											add(ruleAction9, position)
										}
										{
											position40 := position
											if buffer[position] != rune(':') {
												goto l36
											}
											position++
											if !_rules[ruleSpacing]() {
												goto l36
											}
											add(ruleColon, position40)
										}
										add(ruleKeywordSpecifier, position38)
									}
									goto l37
								l36:
									position, tokenIndex = position36, tokenIndex36
								}
							l37:
								{
									position41, tokenIndex41 := position, tokenIndex
									{
										position43 := position
										{
											position44 := position
											{
												position45, tokenIndex45 := position, tokenIndex
												{
													position47 := position
													{
														position48, tokenIndex48 := position, tokenIndex
														if buffer[position] != rune('t') {
															goto l49
														}
														position++
														goto l48
													l49:
														position, tokenIndex = position48, tokenIndex48
														if buffer[position] != rune('T') {
															goto l46
														}
														position++
													}
												l48:
													{
														position50, tokenIndex50 := position, tokenIndex
														if buffer[position] != rune('r') {
															goto l51
														}
														position++
														goto l50
													l51:
														position, tokenIndex = position50, tokenIndex50
														if buffer[position] != rune('R') {
															goto l46
														}
														position++
													}
												l50:
													{
														position52, tokenIndex52 := position, tokenIndex
														if buffer[position] != rune('u') {
															goto l53
														}
														position++
														goto l52
													l53:
														position, tokenIndex = position52, tokenIndex52
														if buffer[position] != rune('U') {
															goto l46
														}
														position++
													}
												l52:
													{
														position54, tokenIndex54 := position, tokenIndex
														if buffer[position] != rune('e') {
															goto l55
														}
														position++
														goto l54
													l55:
														position, tokenIndex = position54, tokenIndex54
														if buffer[position] != rune('E') {
															goto l46
														}
														position++
													}
												l54:
													add(ruleTrue, position47)
												}
												goto l45
											l46:
												position, tokenIndex = position45, tokenIndex45
												{
													position56 := position
													{
														position57, tokenIndex57 := position, tokenIndex
														if buffer[position] != rune('f') {
															goto l58
														}
														position++
														goto l57
													l58:
														position, tokenIndex = position57, tokenIndex57
														if buffer[position] != rune('F') {
															goto l42
														}
														position++
													}
												l57:
													{
														position59, tokenIndex59 := position, tokenIndex
														if buffer[position] != rune('a') {
															goto l60
														}
														position++
														goto l59
													l60:
														position, tokenIndex = position59, tokenIndex59
														if buffer[position] != rune('A') {
															goto l42
														}
														position++
													}
												l59:
													{
														position61, tokenIndex61 := position, tokenIndex
														if buffer[position] != rune('l') {
															goto l62
														}
														position++
														goto l61
													l62:
														position, tokenIndex = position61, tokenIndex61
														if buffer[position] != rune('L') {
															goto l42
														}
														position++
													}
												l61:
													{
														position63, tokenIndex63 := position, tokenIndex
														if buffer[position] != rune('s') {
															goto l64
														}
														position++
														goto l63
													l64:
														position, tokenIndex = position63, tokenIndex63
														if buffer[position] != rune('S') {
															goto l42
														}
														position++
													}
												l63:
													{
														position65, tokenIndex65 := position, tokenIndex
														if buffer[position] != rune('e') {
															goto l66
														}
														position++
														goto l65
													l66:
														position, tokenIndex = position65, tokenIndex65
														if buffer[position] != rune('E') {
															goto l42
														}
														position++
													}
												l65:
													add(ruleFalse, position56)
												}
											}
										l45:
											add(rulePegText, position44)
										}
										{
											position67, tokenIndex67 := position, tokenIndex
											if !_rules[rulePatternChars]() {
												goto l67
											}
											goto l42
										l67:
											position, tokenIndex = position67, tokenIndex67
										}
										if !_rules[ruleSpacing]() {
											goto l42
										}
										add(ruleBoolean, position43)
									}
									{
										add(ruleAction5, position)
									}
									goto l41
								l42:
									position, tokenIndex = position41, tokenIndex41
									{
										position70 := position
										{
											position71 := position
											{
												position72, tokenIndex72 := position, tokenIndex
												{
													position74 := position
													if buffer[position] != rune('-') {
														goto l72
													}
													position++
													add(ruleMinus, position74)
												}
												goto l73
											l72:
												position, tokenIndex = position72, tokenIndex72
											}
										l73:
											{
												position75, tokenIndex75 := position, tokenIndex
												{
													position77 := position
													{
														position78, tokenIndex78 := position, tokenIndex
														if !_rules[ruleIntegralNumber]() {
															goto l78
														}
														goto l79
													l78:
														position, tokenIndex = position78, tokenIndex78
													}
												l79:
													if buffer[position] != rune('.') {
														goto l76
													}
													position++
													if !_rules[ruleIntegralNumber]() {
														goto l76
													}
													add(ruleFloatingNumber, position77)
												}
												goto l75
											l76:
												position, tokenIndex = position75, tokenIndex75
												if !_rules[ruleIntegralNumber]() {
													goto l69
												}
											}
										l75:
											add(rulePegText, position71)
										}
										{
											position80, tokenIndex80 := position, tokenIndex
											if !_rules[rulePatternChars]() {
												goto l80
											}
											goto l69
										l80:
											position, tokenIndex = position80, tokenIndex80
										}
										if !_rules[ruleSpacing]() {
											goto l69
										}
										add(ruleNumber, position70)
									}
									{
										add(ruleAction6, position)
									}
									goto l41
								l69:
									position, tokenIndex = position41, tokenIndex41
									{
										switch buffer[position] {
										case '(':
											if !_rules[ruleNesting]() {
												goto l34
											}
											break
										case '"':
											{
												position83 := position
												if !_rules[ruleQuoteChar]() {
													goto l34
												}
												{
													position84 := position
												l85:
													{
														position86, tokenIndex86 := position, tokenIndex
														{
															position87, tokenIndex87 := position, tokenIndex
															if buffer[position] != rune('"') {
																goto l87
															}
															position++
															goto l86
														l87:
															position, tokenIndex = position87, tokenIndex87
														}
														if !matchDot() {
															goto l86
														}
														goto l85
													l86:
														position, tokenIndex = position86, tokenIndex86
													}
													add(rulePegText, position84)
												}
												if !_rules[ruleQuoteChar]() {
													goto l34
												}
												if !_rules[ruleSpacing]() {
													goto l34
												}
												add(ruleStringLiteral, position83)
											}
											{
												add(ruleAction8, position)
											}
											break
										default:
											{
												position89 := position
												{
													position90 := position
													if !_rules[rulePatternChars]() {
														goto l34
													}
												l91:
													{
														position92, tokenIndex92 := position, tokenIndex
														if !_rules[rulePatternChars]() {
															goto l92
														}
														goto l91
													l92:
														position, tokenIndex = position92, tokenIndex92
													}
													add(rulePegText, position90)
												}
												if !_rules[ruleSpacing]() {
													goto l34
												}
												add(rulePattern, position89)
											}
											{
												add(ruleAction7, position)
											}
											break
										}
									}

								}
							l41:
								add(ruleArgument, position35)
							}
							goto l33
						l34:
							position, tokenIndex = position34, tokenIndex34
						}
						{
							add(ruleAction4, position)
						}
						add(ruleFunctionCall, position22)
					}
					goto l20
				l21:
					position, tokenIndex = position20, tokenIndex20
					if !_rules[ruleNesting]() {
						goto l18
					}
				}
			l20:
				add(ruleExpression, position19)
			}
			return true
		l18:
			position, tokenIndex = position18, tokenIndex18
			return false
		},
		/* 4 FunctionCall <- <((Identifier / Operator) Action3 Argument* Action4)> */
		nil,
		/* 5 Argument <- <(KeywordSpecifier? ((Boolean Action5) / (Number Action6) / ((&('(') Nesting) | (&('"') (StringLiteral Action8)) | (&('$' | '*' | ',' | '-' | '.' | '/' | '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' | '?' | 'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '[' | '\\' | ']' | '^' | '_' | 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' | '{' | '}') (Pattern Action7)))))> */
		nil,
		/* 6 KeywordSpecifier <- <(Identifier Action9 Colon)> */
		nil,
		/* 7 Nesting <- <(LParenthesis Pipeline RParenthesis)> */
		func() bool {
			position98, tokenIndex98 := position, tokenIndex
			{
				position99 := position
				{
					position100 := position
					if buffer[position] != rune('(') {
						goto l98
					}
					position++
					if !_rules[ruleSpacing]() {
						goto l98
					}
					add(ruleLParenthesis, position100)
				}
				if !_rules[rulePipeline]() {
					goto l98
				}
				{
					position101 := position
					if buffer[position] != rune(')') {
						goto l98
					}
					position++
					if !_rules[ruleSpacing]() {
						goto l98
					}
					add(ruleRParenthesis, position101)
				}
				add(ruleNesting, position99)
			}
			return true
		l98:
			position, tokenIndex = position98, tokenIndex98
			return false
		},
		/* 8 Spacing <- <((&('#') Comment) | (&('\n' | '\r') EOL) | (&('\t' | ' ') Space))*> */
		func() bool {
			{
				position103 := position
			l104:
				{
					position105, tokenIndex105 := position, tokenIndex
					{
						switch buffer[position] {
						case '#':
							{
								position107 := position
								{
									position108 := position
									if buffer[position] != rune('#') {
										goto l105
									}
									position++
									add(ruleCommentStart, position108)
								}
							l109:
								{
									position110, tokenIndex110 := position, tokenIndex
									{
										position111, tokenIndex111 := position, tokenIndex
										if !_rules[ruleEOL]() {
											goto l111
										}
										goto l110
									l111:
										position, tokenIndex = position111, tokenIndex111
									}
									if !matchDot() {
										goto l110
									}
									goto l109
								l110:
									position, tokenIndex = position110, tokenIndex110
								}
								add(ruleComment, position107)
							}
							break
						case '\n', '\r':
							if !_rules[ruleEOL]() {
								goto l105
							}
							break
						default:
							{
								position112 := position
								{
									position113, tokenIndex113 := position, tokenIndex
									if buffer[position] != rune(' ') {
										goto l114
									}
									position++
									goto l113
								l114:
									position, tokenIndex = position113, tokenIndex113
									if buffer[position] != rune('\t') {
										goto l105
									}
									position++
								}
							l113:
								add(ruleSpace, position112)
							}
							break
						}
					}

					goto l104
				l105:
					position, tokenIndex = position105, tokenIndex105
				}
				add(ruleSpacing, position103)
			}
			return true
		},
		/* 9 Space <- <(' ' / '\t')> */
		nil,
		/* 10 EOL <- <(('\r' '\n') / '\n' / '\r')> */
		func() bool {
			position116, tokenIndex116 := position, tokenIndex
			{
				position117 := position
				{
					position118, tokenIndex118 := position, tokenIndex
					if buffer[position] != rune('\r') {
						goto l119
					}
					position++
					if buffer[position] != rune('\n') {
						goto l119
					}
					position++
					goto l118
				l119:
					position, tokenIndex = position118, tokenIndex118
					if buffer[position] != rune('\n') {
						goto l120
					}
					position++
					goto l118
				l120:
					position, tokenIndex = position118, tokenIndex118
					if buffer[position] != rune('\r') {
						goto l116
					}
					position++
				}
			l118:
				add(ruleEOL, position117)
			}
			return true
		l116:
			position, tokenIndex = position116, tokenIndex116
			return false
		},
		/* 11 Comment <- <(CommentStart (!EOL .)*)> */
		nil,
		/* 12 CommentStart <- <'#'> */
		nil,
		/* 13 Identifier <- <(<(IdentifierStart IdentifierChars*)> Spacing)> */
		func() bool {
			position123, tokenIndex123 := position, tokenIndex
			{
				position124 := position
				{
					position125 := position
					if !_rules[ruleIdentifierStart]() {
						goto l123
					}
				l126:
					{
						position127, tokenIndex127 := position, tokenIndex
						if !_rules[ruleIdentifierChars]() {
							goto l127
						}
						goto l126
					l127:
						position, tokenIndex = position127, tokenIndex127
					}
					add(rulePegText, position125)
				}
				if !_rules[ruleSpacing]() {
					goto l123
				}
				add(ruleIdentifier, position124)
			}
			return true
		l123:
			position, tokenIndex = position123, tokenIndex123
			return false
		},
		/* 14 IdentifierStart <- <((&('_') '_') | (&('A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z') [A-Z]) | (&('a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z') [a-z]))> */
		func() bool {
			position128, tokenIndex128 := position, tokenIndex
			{
				position129 := position
				{
					switch buffer[position] {
					case '_':
						if buffer[position] != rune('_') {
							goto l128
						}
						position++
						break
					case 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z':
						if c := buffer[position]; c < rune('A') || c > rune('Z') {
							goto l128
						}
						position++
						break
					default:
						if c := buffer[position]; c < rune('a') || c > rune('z') {
							goto l128
						}
						position++
						break
					}
				}

				add(ruleIdentifierStart, position129)
			}
			return true
		l128:
			position, tokenIndex = position128, tokenIndex128
			return false
		},
		/* 15 IdentifierChars <- <((&('\\') '\\') | (&('/') '/') | (&('-') '-') | (&('.') '.') | (&('0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9') [0-9]) | (&('A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | '_' | 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z') IdentifierStart))> */
		func() bool {
			position131, tokenIndex131 := position, tokenIndex
			{
				position132 := position
				{
					switch buffer[position] {
					case '\\':
						if buffer[position] != rune('\\') {
							goto l131
						}
						position++
						break
					case '/':
						if buffer[position] != rune('/') {
							goto l131
						}
						position++
						break
					case '-':
						if buffer[position] != rune('-') {
							goto l131
						}
						position++
						break
					case '.':
						if buffer[position] != rune('.') {
							goto l131
						}
						position++
						break
					case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
						if c := buffer[position]; c < rune('0') || c > rune('9') {
							goto l131
						}
						position++
						break
					default:
						if !_rules[ruleIdentifierStart]() {
							goto l131
						}
						break
					}
				}

				add(ruleIdentifierChars, position132)
			}
			return true
		l131:
			position, tokenIndex = position131, tokenIndex131
			return false
		},
		/* 16 Operator <- <(<OperatorSymbols> Spacing)> */
		nil,
		/* 17 OperatorSymbols <- <(('<' '=') / ('>' '=') / ((&('>') '>') | (&('!') ('!' '=')) | (&('=') ('=' '=')) | (&('<') '<')))> */
		nil,
		/* 18 Boolean <- <(<(True / False)> !PatternChars Spacing)> */
		nil,
		/* 19 True <- <(('t' / 'T') ('r' / 'R') ('u' / 'U') ('e' / 'E'))> */
		nil,
		/* 20 False <- <(('f' / 'F') ('a' / 'A') ('l' / 'L') ('s' / 'S') ('e' / 'E'))> */
		nil,
		/* 21 Number <- <(<(Minus? (FloatingNumber / IntegralNumber))> !PatternChars Spacing)> */
		nil,
		/* 22 IntegralNumber <- <[0-9]+> */
		func() bool {
			position140, tokenIndex140 := position, tokenIndex
			{
				position141 := position
				if c := buffer[position]; c < rune('0') || c > rune('9') {
					goto l140
				}
				position++
			l142:
				{
					position143, tokenIndex143 := position, tokenIndex
					if c := buffer[position]; c < rune('0') || c > rune('9') {
						goto l143
					}
					position++
					goto l142
				l143:
					position, tokenIndex = position143, tokenIndex143
				}
				add(ruleIntegralNumber, position141)
			}
			return true
		l140:
			position, tokenIndex = position140, tokenIndex140
			return false
		},
		/* 23 FloatingNumber <- <(IntegralNumber? '.' IntegralNumber)> */
		nil,
		/* 24 Minus <- <'-'> */
		nil,
		/* 25 StringLiteral <- <(QuoteChar <(!'"' .)*> QuoteChar Spacing)> */
		nil,
		/* 26 QuoteChar <- <'"'> */
		func() bool {
			position147, tokenIndex147 := position, tokenIndex
			{
				position148 := position
				if buffer[position] != rune('"') {
					goto l147
				}
				position++
				add(ruleQuoteChar, position148)
			}
			return true
		l147:
			position, tokenIndex = position147, tokenIndex147
			return false
		},
		/* 27 Pattern <- <(<PatternChars+> Spacing)> */
		nil,
		/* 28 PatternChars <- <(IdentifierChars / GlobSymbols)> */
		func() bool {
			position150, tokenIndex150 := position, tokenIndex
			{
				position151 := position
				{
					position152, tokenIndex152 := position, tokenIndex
					if !_rules[ruleIdentifierChars]() {
						goto l153
					}
					goto l152
				l153:
					position, tokenIndex = position152, tokenIndex152
					{
						position154 := position
						{
							switch buffer[position] {
							case '$':
								if buffer[position] != rune('$') {
									goto l150
								}
								position++
								break
							case '^':
								if buffer[position] != rune('^') {
									goto l150
								}
								position++
								break
							case ',':
								if buffer[position] != rune(',') {
									goto l150
								}
								position++
								break
							case '?':
								if buffer[position] != rune('?') {
									goto l150
								}
								position++
								break
							case '*':
								if buffer[position] != rune('*') {
									goto l150
								}
								position++
								break
							case ']':
								if buffer[position] != rune(']') {
									goto l150
								}
								position++
								break
							case '[':
								if buffer[position] != rune('[') {
									goto l150
								}
								position++
								break
							case '}':
								if buffer[position] != rune('}') {
									goto l150
								}
								position++
								break
							default:
								if buffer[position] != rune('{') {
									goto l150
								}
								position++
								break
							}
						}

						add(ruleGlobSymbols, position154)
					}
				}
			l152:
				add(rulePatternChars, position151)
			}
			return true
		l150:
			position, tokenIndex = position150, tokenIndex150
			return false
		},
		/* 29 GlobSymbols <- <((&('$') '$') | (&('^') '^') | (&(',') ',') | (&('?') '?') | (&('*') '*') | (&(']') ']') | (&('[') '[') | (&('}') '}') | (&('{') '{'))> */
		nil,
		/* 30 Semicolon <- <(';' Spacing)> */
		nil,
		/* 31 Equals <- <('=' Spacing)> */
		nil,
		/* 32 Pipe <- <('|' Spacing)> */
		nil,
		/* 33 LParenthesis <- <('(' Spacing)> */
		nil,
		/* 34 RParenthesis <- <(')' Spacing)> */
		nil,
		/* 35 Colon <- <(':' Spacing)> */
		nil,
		/* 36 EOF <- <!.> */
		nil,
		/* 38 Action0 <- <{ p.newMacro(text) }> */
		nil,
		/* 39 Action1 <- <{ p.newPipeline() }> */
		nil,
		/* 40 Action2 <- <{ p.endPipeline() }> */
		nil,
		/* 41 Action3 <- <{ p.newExpression(text) }> */
		nil,
		/* 42 Action4 <- <{ p.endExpression() }> */
		nil,
		/* 43 Action5 <- <{ p.newBooleanArgument(text)        }> */
		nil,
		/* 44 Action6 <- <{ p.newNumericArgument(text)        }> */
		nil,
		/* 45 Action7 <- <{ p.newPatternArgument(text)        }> */
		nil,
		/* 46 Action8 <- <{ p.newStringLiteralArgument(text)  }> */
		nil,
		/* 47 Action9 <- <{ p.newKeywordArgument(text) }> */
		nil,
		nil,
	}
	p.rules = _rules
}
