/*
 * Copyright (c) 2019 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package m3ql

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/m3db/m3/src/query/errors"
)

var (
	funcMutex    sync.RWMutex
	functions    = map[string]*Function{}
	descriptions []FunctionDescription
)

// FunctionDescriptions returns all descriptions
// for registered functions (should not return aliases)
func FunctionDescriptions() []FunctionDescription {
	return descriptions
}

// registerFunction is used to register a function under a specific name.
func registerFunction(f interface{}) (*Function, error) {
	fn, err := buildFunction(f)
	if err != nil {
		return nil, err
	}

	funcMutex.Lock()
	defer funcMutex.Unlock()

	if functions[fn.name] != nil {
		return nil, fmt.Errorf("function %s already registered", fn.name)
	}

	functions[fn.name] = fn
	descriptions = append(descriptions, fn)
	return fn, nil
}

// MustRegisterFunction registers a function, issuing a panic if the function cannot be registered.
func MustRegisterFunction(f interface{}) *Function {
	if fn, err := registerFunction(f); err != nil {
		panic(err)
	} else {
		return fn
	}
}

// registerAliasedFunction is used to register a function under an alias.
func registerAliasedFunction(alias string, f interface{}) error {
	fname, err := functionName(f)
	if err != nil {
		return err
	}

	funcMutex.Lock()
	defer funcMutex.Unlock()

	if functions[alias] != nil {
		return fmt.Errorf("function %s already registered", alias)
	}

	fn := functions[fname]
	if fn == nil {
		return fmt.Errorf("target function %s not registered", fname)
	}

	functions[alias] = fn
	return nil
}

// MustRegisterAliasedFunction registers a function under an alias, issuing a panic if the function
// cannot be registered.
func MustRegisterAliasedFunction(fname string, f interface{}) {
	if err := registerAliasedFunction(fname, f); err != nil {
		panic(err)
	}
}

// FindFunction finds a function with the given name.
func FindFunction(name string) *Function {
	funcMutex.RLock()
	defer funcMutex.RUnlock()

	return functions[name]
}

// Keyword is a keyword function parameter.
type Keyword struct {
	Key   string
	Value interface{}
}

func (k Keyword) String() string {
	return fmt.Sprintf("%s:%v", k.Key, k.Value)
}

// StringKeyword is a keyword function parameter with string value.
// NB(bl): This tells the compiler not to modify string values that look
// like non-strings, e.g. large uuid's, version #'s.
type StringKeyword struct {
	Key   string
	Value string
}

func (k StringKeyword) String() string {
	return k.Key + ":" + k.Value
}

// TypeList is a set of reflect.Type objects.
type TypeList []reflect.Type

// contains checks whether the type set contains the given type.
func (tl TypeList) contains(t reflect.Type) bool {
	for i := range tl {
		if tl[i] == t {
			return true
		}
	}

	return false
}

var (
	boolType          = reflect.TypeOf(false)
	errorType         = reflect.TypeOf((*error)(nil)).Elem()
	expressionType    = reflect.TypeOf((*Expression)(nil)).Elem()
	float64Type       = reflect.TypeOf(float64(0))
	intType           = reflect.TypeOf(int(0))
	stringKeywordType = reflect.TypeOf(StringKeyword{})
	keywordType       = reflect.TypeOf(Keyword{})
	pipelineType      = reflect.TypeOf((*Pipeline)(nil)).Elem()
	stringType        = reflect.TypeOf("")
	interfaceType     = reflect.TypeOf([]genericInterface{}).Elem()

	// Types which are allowed as function parameters during compile-time.
	allowableTypes = TypeList{
		boolType,
		float64Type,
		intType,
		stringKeywordType,
		keywordType,
		pipelineType,
		stringType,
		interfaceType,
	}
)

var (
	errNonFunction   = errors.NewParseErr(errors.New("not a function"))
	errVariadic      = errors.NewParseErr(errors.New("last parameter for a variadic function must be a slice"))
	errInvalidReturn = errors.NewParseErr(errors.New("functions must return an Expression and an error"))
)

// FunctionDescription provides an interface
// for describing a function and its parameters
type FunctionDescription interface {
	Name() string
	IsVariadic() bool
	Parameters() []reflect.Type
}

// Function contains a function to invoke along with metadata about
// the function's argument and return type.
type Function struct {
	name     string
	fn       reflect.Value
	in       []reflect.Type
	defaults map[uint8]interface{}
	rv       reflect.Type
	variadic bool
}

// Name returns the name of the function
func (f *Function) Name() string {
	return f.name
}

// IsVariadic Returns whether the function supports
// an arbitrary length of input parameters
func (f *Function) IsVariadic() bool {
	return f.variadic
}

// Parameters returns a list of the functions
// input parameters
func (f *Function) Parameters() []reflect.Type {
	return f.in
}

// WithDefaultParams provides default parameters for functions.
func (f *Function) WithDefaultParams(defaultParams map[uint8]interface{}) *Function {
	for index := range defaultParams {
		if int(index) < 0 || int(index) > len(f.in) {
			panic(fmt.Sprintf("default parameter #%d for %s is out-of-range", index, f.name))
		}
	}

	f.defaults = defaultParams

	return f
}

func functionName(f interface{}) (string, error) {
	v := reflect.ValueOf(f)
	t := v.Type()

	if t.Kind() != reflect.Func {
		return "", errNonFunction
	}

	nameParts := strings.Split(runtime.FuncForPC(v.Pointer()).Name(), ".")

	return nameParts[len(nameParts)-1], nil
}

// buildFunction takes a reflection reference to a function and returns
// the function metadata.
func buildFunction(f interface{}) (*Function, error) {
	fname, err := functionName(f)
	if err != nil {
		return nil, err
	}

	v := reflect.ValueOf(f)
	t := v.Type()

	in := make([]reflect.Type, 0, t.NumIn())

	for i := 0; i < t.NumIn(); i++ {
		inArg := t.In(i)

		if t.IsVariadic() && i == (t.NumIn()-1) {
			// Type-transform the last parameter slice type of a variadic function into the slice's element type.
			inArg = inArg.Elem()
		}

		if !allowableTypes.contains(inArg) {
			return nil, fmt.Errorf("invalid argument type %d for %s: %s is not supported", i, fname, inArg.String())
		}

		in = append(in, inArg)
	}

	if t.NumOut() != 2 {
		return nil, errInvalidReturn
	}

	rv := []reflect.Type{t.Out(0), t.Out(1)}

	if !rv[0].Implements(expressionType) || rv[1] != errorType {
		return nil, fmt.Errorf("invalid return type for %s: (%s, %s)", fname, rv[0].String(), rv[1].String())
	}

	return &Function{name: fname, fn: v, in: in, rv: rv[0], variadic: t.IsVariadic()}, nil
}

// Call calls the function with non-reflected values.
func (f *Function) Call(args ...interface{}) (Expression, error) {
	values := make([]reflect.Value, len(args))

	for i := range args {
		values[i] = reflect.ValueOf(args[i])
	}

	rv, err := f.reflectCall(values)
	if err != nil {
		return nil, err
	}

	return rv.Interface().(Expression), nil
}

// MustCall calls the function with non-reflected values, issuing a panic if function call yielded any
// kind of error.
func (f *Function) MustCall(args ...interface{}) Expression {
	if expression, err := f.Call(args...); err != nil {
		panic(err)
	} else {
		return expression
	}
}

// reflectCall calls the function with reflected values, passing in the provided context and parameters.
func (f *Function) reflectCall(args []reflect.Value) (reflect.Value, error) {
	in := make([]reflect.Value, 0, len(args))

	for _, arg := range args {
		in = append(in, arg)
	}

	out := f.fn.Call(in)
	outVal, errVal := out[0], out[1]

	var err error

	if !errVal.IsNil() {
		err = errVal.Interface().(error)
		return outVal, err
	}

	return outVal, nil
}

// FunctionArgument is an argument to a function that gets resolved at compile-time.
type FunctionArgument interface {
	Compile() (reflect.Value, error)
	CompatibleWith(reflectType reflect.Type) bool
	String() string
	NormalizedString() string
	TypeName() string
	Raw() string
}

// A constantArgument is a function argument that is a constant value.
type constantArgument struct {
	value reflect.Value
	raw   string // the raw input string
}

func newConstArg(i interface{}, text string) FunctionArgument {
	return constantArgument{value: reflect.ValueOf(i), raw: text}
}
func newBoolConst(b bool, text string) FunctionArgument {
	return constantArgument{value: reflect.ValueOf(b), raw: text}
}
func newStringConst(s string) FunctionArgument {
	return constantArgument{value: reflect.ValueOf(s), raw: s}
}
func newFloat64Const(n float64, text string) FunctionArgument {
	return constantArgument{value: reflect.ValueOf(n), raw: text}
}
func newIntConst(n int, text string) FunctionArgument {
	return constantArgument{value: reflect.ValueOf(n), raw: text}
}

func (c constantArgument) Compile() (reflect.Value, error) {
	return c.value, nil
}

func (c constantArgument) CompatibleWith(reflectType reflect.Type) bool {
	return c.value.Type() == reflectType || reflectType == interfaceType
}

func (c constantArgument) String() string {
	return fmt.Sprintf("%v", c.value.Interface())
}

func (c constantArgument) NormalizedString() string {
	return c.String()
}

func (c constantArgument) TypeName() string {
	return c.value.Type().Name()
}

func (c constantArgument) Raw() string {
	return c.raw
}

type keywordArgType int

const (
	_ keywordArgType = iota // ignore default value
	genericArg
	strArg
)

type keywordArgument struct {
	key     string
	value   FunctionArgument
	argType keywordArgType
}

func newKeywordConst(key string, value FunctionArgument, argType keywordArgType) FunctionArgument {
	return keywordArgument{key: key, value: value, argType: argType}
}

func (c keywordArgument) Compile() (reflect.Value, error) {
	switch c.argType {
	case strArg:
		return reflect.ValueOf(StringKeyword{Key: c.key, Value: c.value.Raw()}), nil
	case genericArg:
		value, err := c.value.Compile()

		if err != nil {
			return reflect.Value{}, err
		}
		return reflect.ValueOf(Keyword{Key: c.key, Value: value.Interface()}), nil
	default:
		panic(fmt.Sprintf("Unknown keyword argument argType: %v", c.argType))
	}
}

func (c keywordArgument) CompatibleWith(reflectType reflect.Type) bool {
	return reflect.TypeOf(Keyword{}) == reflectType ||
		reflect.TypeOf(StringKeyword{}) == reflectType
}

func (c keywordArgument) String() string {
	return fmt.Sprintf("%s:%s", c.key, c.value)
}

func (c keywordArgument) NormalizedString() string {
	return c.String()
}

func (c keywordArgument) TypeName() string {
	return "tag"
}

func (c keywordArgument) Raw() string {
	return c.key + ":" + c.value.Raw()
}

// A functionCall is an actual call to a function, with resolution for arguments.
type functionCall struct {
	fn *Function
	in []FunctionArgument
}

func newFunctionCall(fn *Function, in []FunctionArgument) *functionCall {
	return &functionCall{fn: fn, in: in}
}

// curry evaluates the function call and returns the result as a reflect.Value
func (call *functionCall) curry() (reflect.Value, error) {
	values := make([]reflect.Value, len(call.in))

	for i, argument := range call.in {
		value, err := argument.Compile()

		if err != nil {
			return reflect.Value{}, err
		}

		values[i] = value
	}

	return call.fn.reflectCall(values)
}
