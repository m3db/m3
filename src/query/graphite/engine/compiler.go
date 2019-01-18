package engine

import (
	"fmt"
	"math"
	"reflect"
	"strconv"

	"code.uber.internal/infra/statsdex/query/lexer"
)

// compile converts an input stream into the corresponding Expression
func compile(input string) (Expression, error) {
	booleanLiterals := map[string]lexer.TokenType{
		"true":  lexer.True,
		"false": lexer.False,
	}
	lex, tokens := lexer.NewLexer(input, booleanLiterals)
	go lex.Run()

	c := compiler{input: input, tokens: tokens}
	expr, err := c.compileExpression()

	// Exhaust all tokens until closed or else lexer won't close
	for range tokens {
	}

	return expr, err
}

// A compiler converts an input string into an executable Expression
type compiler struct {
	input  string
	tokens chan *lexer.Token
}

// compileExpression compiles a top level expression
func (c *compiler) compileExpression() (Expression, error) {
	token := <-c.tokens
	if token == nil {
		return noopExpression{}, nil
	}

	var expr Expression
	switch token.TokenType() {
	case lexer.Pattern:
		expr = newFetchExpression(token.Value())

	case lexer.Identifier:
		fc, err := c.compileFunctionCall(token.Value(), nil)
		if err != nil {
			return nil, err
		}

		expr, err = newFuncExpression(fc)
		if err != nil {
			return nil, err
		}

	default:
		return nil, c.errorf("unexpected value %s", token.Value())
	}

	if token := <-c.tokens; token != nil {
		return nil, c.errorf("extra data %s", token.Value())
	}

	return expr, nil
}

// compileFunctionCall compiles a function call
func (c *compiler) compileFunctionCall(fname string, nextToken *lexer.Token) (*functionCall, error) {
	fn := findFunction(fname)
	if fn == nil {
		return nil, c.errorf("could not find function named %s", fname)
	}

	if nextToken != nil {
		if nextToken.TokenType() != lexer.LParenthesis {
			return nil, c.errorf("expected %v but encountered %s", lexer.LParenthesis, nextToken.Value())
		}
	} else {
		if _, err := c.expectToken(lexer.LParenthesis); err != nil {
			return nil, err
		}
	}

	argTypes := fn.in
	var args []funcArg

	// build up arguments for function call
	for {
		// if not variadic, function should be complete after reading len(argTypes) arguments
		if !fn.variadic && len(args) == len(argTypes) {
			_, err := c.expectToken(lexer.RParenthesis)
			if err != nil {
				return nil, err
			}
			break
		}

		argType := argTypes[int(math.Min(float64(len(args)), float64(len(argTypes)-1)))]
		nextArg, foundRParen, err := c.compileArg(fn.name, len(args), argType)
		if err != nil {
			return nil, err
		}
		if foundRParen {
			break
		}

		args = append(args, nextArg)
	}

	// fill in defaults arguments for those not supplied by user explicitly
	for len(args) < len(argTypes) {
		defaultValue, ok := fn.defaults[uint8(len(args)+1)]
		if !ok {
			break
		}

		args = append(args, newConstArg(defaultValue))
	}

	// all required argument types should be filled with values now
	if len(args) < len(argTypes) {
		variadicComment := ""
		if fn.variadic {
			variadicComment = "at least "
		}
		return nil, c.errorf("invalid number of arguments for %s; expected %s%d, received %d",
			fn.name, variadicComment, len(argTypes), len(args))
	}

	return &functionCall{f: fn, in: args}, nil
}

// compileArg parses and compiles a single argument
func (c *compiler) compileArg(fname string, index int,
	reflectType reflect.Type) (arg funcArg, foundRParen bool, err error) {
	token := <-c.tokens
	if token == nil {
		return nil, false, c.errorf("unexpected eof while parsing %s", fname)
	}

	if token.TokenType() == lexer.RParenthesis {
		return nil, true, nil
	}

	if index > 0 {
		if token.TokenType() != lexer.Comma {
			return nil, false, c.errorf("error parsing %s expected ',' received '%s'",
				fname, token.Value())
		}

		if token = <-c.tokens; token == nil {
			return nil, false, c.errorf("unexpected eof while parsing %s", fname)
		}
	}

	arg, err = c.convertTokenToArg(token, reflectType)
	if err != nil {
		return nil, false, c.errorf("invalid function call %s, arg %d: %v", fname, index, err)
	}

	if !arg.CompatibleWith(reflectType) {
		return nil, false, c.errorf("invalid function call %s, arg %d: expected a %s, received '%s'",
			fname, index, reflectType.Name(), arg)
	}

	return arg, false, nil
}

// convertTokenToArg converts the given token into the corresponding argument
func (c *compiler) convertTokenToArg(token *lexer.Token, reflectType reflect.Type) (funcArg, error) {
	switch token.TokenType() {
	case lexer.Number:
		n, err := strconv.ParseFloat(token.Value(), 64)
		if err != nil {
			return nil, err
		}

		if reflectType.Kind() == reflect.Int {
			return newIntConst(int(n)), nil
		}

		return newFloat64Const(n), nil
	case lexer.String:
		return newStringConst(token.Value()), nil
	case lexer.Pattern:
		return newFetchExpression(token.Value()), nil
	case lexer.True, lexer.False:
		b, err := strconv.ParseBool(token.Value())
		if err != nil {
			return nil, err
		}
		return newBoolConst(b), nil
	case lexer.Identifier:
		currentToken := token.Value()

		// handle named arguments
		nextToken := <-c.tokens
		if nextToken == nil {
			return nil, c.errorf("unexpected eof, %s should be followed by = or (", currentToken)
		}
		if nextToken.TokenType() == lexer.Equal {
			// TODO(xichen): check if currentToken matches the expected parameter name
			tokenAfterNext := <-c.tokens
			if tokenAfterNext == nil {
				return nil, c.errorf("unexpected eof, named argument %s should be followed by its value", currentToken)
			}
			return c.convertTokenToArg(tokenAfterNext, reflectType)
		}

		return c.compileFunctionCall(currentToken, nextToken)
	default:
		return nil, c.errorf("%s not valid", token.Value())
	}
}

// expectToken reads the next token and confirms it is the expected type before returning it
func (c *compiler) expectToken(expectedType lexer.TokenType) (*lexer.Token, error) {
	token := <-c.tokens
	if token == nil {
		return nil, c.errorf("expected %v but encountered eof", expectedType)
	}

	if token.TokenType() != expectedType {
		return nil, c.errorf("expected %v but encountered %s", expectedType, token.Value())
	}

	return token, nil
}

// errorf returns a formatted error vfrom the compiler
func (c *compiler) errorf(msg string, args ...interface{}) error {
	return fmt.Errorf("invalid expression '%s': %s", c.input, fmt.Sprintf(msg, args...))
}

// ExtractFetchExpressions extracts timeseries fetch expressions from the given query
func ExtractFetchExpressions(s string) ([]string, error) {
	expr, err := compile(s)
	if err != nil {
		return nil, err
	}

	var targets []string
	extractFetchExpressions(expr, &targets)
	return targets, nil
}

func extractFetchExpressions(expr Expression, targets *[]string) {
	switch v := expr.(type) {
	case *funcExpression:
		extractFetchExpressionsFromFuncCall(v.call, targets)
	case *fetchExpression:
		*targets = append(*targets, v.pathArg.path)
	}
}

func extractFetchExpressionsFromFuncCall(call *functionCall, targets *[]string) {
	for _, arg := range call.in {
		switch varg := arg.(type) {
		case *functionCall:
			extractFetchExpressionsFromFuncCall(varg, targets)
		case Expression:
			extractFetchExpressions(varg, targets)
		}
	}
}
