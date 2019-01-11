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
	"math"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/m3db/m3/src/query/errors"
)

type keywordBuilder struct {
	key string
}

func (b *keywordBuilder) build(
	value FunctionArgument, argType keywordArgType) FunctionArgument {

	return newKeywordConst(b.key, value, argType)
}

type functionBuilder struct {
	fn   *Function
	args []FunctionArgument

	// Helpers.
	kw *keywordBuilder
}

func (b *functionBuilder) build() (Expression, error) {
	// Fill in defaults arguments for those not supplied by user explicitly.
	for len(b.args) < len(b.fn.in) {
		if v, exists := b.fn.defaults[uint8(len(b.args))]; !exists {
			break
		} else {
			b.args = append(b.args, newConstArg(v, fmt.Sprintf("%v", v)))
		}
	}

	// All required argument types should be filled with values now.
	if !b.fn.variadic && len(b.args) < len(b.fn.in) {
		return nil, errors.NewParseErr(fmt.Errorf(
			"function %s expects at least %d arguments, received %d",
			b.fn.name, len(b.fn.in), len(b.args)))
	}

	var r Expression

	if v, err := newFunctionCall(b.fn, b.args).curry(); err == nil {
		r = v.Interface().(Expression)
	} else {
		return nil, err
	}

	return r, nil
}

func (b *functionBuilder) addKeywordArgument(text string) error {
	b.kw = &keywordBuilder{key: text}
	return nil
}

func (b *functionBuilder) addBooleanArgument(text string) error {
	v, err := strconv.ParseBool(text)

	if err != nil {
		return errors.NewParseErr(fmt.Errorf(
			"unable to parse a boolean argument for %s from %s",
			b.fn.name, text))
	}

	return b.addArgument(newBoolConst(v, text))
}

func (b *functionBuilder) addNumericArgument(text string) error {
	v, err := strconv.ParseFloat(text, 64)

	if err != nil {
		return errors.NewParseErr(fmt.Errorf(
			"unable to parse a numeric argument for %s from %s",
			b.fn.name, text))
	}

	switch c := b.currentType(); c.Kind() {
	case reflect.String:
		return b.addArgument(newStringConst(text))
	case reflect.Int:
		return b.addArgument(newIntConst(int(v), text))
	case reflect.Float64:
		return b.addArgument(newFloat64Const(v, text))
	}

	return b.addArgument(newConstArg(v, text))
}

func (b *functionBuilder) addPatternArgument(text string) error {
	return b.addArgument(newStringConst(text))
}

func (b *functionBuilder) addStringLiteralArgument(text string) error {
	return b.addArgument(newStringConst(text))
}

func (b *functionBuilder) addPipelineArgument(pipeline FunctionArgument) error {
	return b.addArgument(pipeline)
}

var none = reflect.TypeOf(struct{}{})

func (b *functionBuilder) currentType() reflect.Type {
	if !b.fn.variadic && len(b.args) == len(b.fn.in) {
		return none
	}

	return b.fn.in[int(math.Min(float64(len(b.args)), float64(len(b.fn.in)-1)))]
}

func (b *functionBuilder) addArgument(arg FunctionArgument) error {
	if !b.fn.variadic && len(b.args) == len(b.fn.in) {
		return errors.NewParseErr(fmt.Errorf(
			"function %s received unexpected argument %d: %s",
			b.fn.name, len(b.args), arg))
	}

	if b.kw != nil {
		var argType keywordArgType

		switch b.currentType() {
		case reflect.TypeOf(StringKeyword{}):
			argType = strArg
		default:
			argType = genericArg
		}

		arg, b.kw = b.kw.build(arg, argType), nil
	}

	if !arg.CompatibleWith(b.currentType()) {
		return errors.NewParseErr(fmt.Errorf(
			"function %s expects argument %d of type %s, received '%s'",
			b.fn.name, len(b.args), b.currentType().String(), arg))
	}

	b.args = append(b.args, arg)

	return nil
}

type pipelineBuilder struct {
	pipeline Pipeline

	// Helpers.
	fn *functionBuilder
}

type macroBuilder struct {
	name string
}

func (b *macroBuilder) build(pipeline Pipeline) (*Function, error) {
	fn, _ := buildFunction(func() (Expression, error) {
		return pipeline, nil
	})

	// Replace the meaningless closure function name with macro name.
	fn.name = b.name

	return fn, nil
}

type scriptBuilder struct {
	stack     []pipelineBuilder
	macroDefs map[string]*Function

	// Helpers.
	macro *macroBuilder
}

func (p *scriptBuilder) newPipeline() {
	p.stack = append([]pipelineBuilder{{pipeline: NewOptimizePipeline()}}, p.stack...)
}

func (p *scriptBuilder) endPipeline() {
	if p.stack[0].fn != nil {
		panic(fmt.Errorf("found incomplete function call in a pipeline"))
	}

	r := p.stack[0].pipeline

	if len(p.stack) == 1 {
		if p.macro != nil {
			// This is a pipeline for a macro being built.
			if fn, err := p.macro.build(r); err != nil {
				panic(err)
			} else {
				p.macroDefs[p.macro.name], p.macro = fn, nil
			}
		} else {
			// This was the main pipeline, leave it there.
			return
		}
	} else if parent := p.stack[1]; parent.fn == nil {
		// Syntax sugar for execute.
		b := &functionBuilder{fn: FindFunction("execute")}
		b.addPipelineArgument(r)

		if e, err := b.build(); err != nil {
			panic(err)
		} else {
			parent.pipeline.AddStep(NewPipelineStep(e, b.fn, b.args))
		}
	} else if err := parent.fn.addPipelineArgument(r); err != nil {
		panic(err)
	}

	p.stack = p.stack[1:]
}

func (p *scriptBuilder) newExpression(text string) {
	var fn *Function

	if macro, exists := p.macroDefs[text]; exists {
		fn = macro
	} else if fn = FindFunction(text); fn == nil {
		panic(fmt.Errorf("could not find function or macro named %s", text))
	}

	p.stack[0].fn = &functionBuilder{fn: fn}
}

func (p *scriptBuilder) endExpression() {
	b := p.stack[0].fn

	e, err := b.build()
	if err != nil {
		panic(err)
	}

	p.stack[0].fn = nil
	p.stack[0].pipeline.AddStep(NewPipelineStep(e, b.fn, b.args))
}

func (p *scriptBuilder) newKeywordArgument(text string) {
	if err := p.stack[0].fn.addKeywordArgument(text); err != nil {
		panic(err)
	}
}

func (p *scriptBuilder) newBooleanArgument(text string) {
	if err := p.stack[0].fn.addBooleanArgument(text); err != nil {
		panic(err)
	}
}

func (p *scriptBuilder) newNumericArgument(text string) {
	if err := p.stack[0].fn.addNumericArgument(text); err != nil {
		panic(err)
	}
}

func (p *scriptBuilder) newPatternArgument(text string) {
	if err := p.stack[0].fn.addPatternArgument(text); err != nil {
		panic(err)
	}
}

func (p *scriptBuilder) newStringLiteralArgument(text string) {
	literal := strings.TrimFunc(text, func(r rune) bool {
		return r == '"' || unicode.IsSpace(r)
	})
	if err := p.stack[0].fn.addStringLiteralArgument(literal); err != nil {
		panic(err)
	}
}

func (p *scriptBuilder) newMacro(text string) {
	p.macro = &macroBuilder{name: text}
}

func (p *scriptBuilder) build() (Pipeline, error) {
	if len(p.stack) != 1 {
		return nil, fmt.Errorf("found incomplete pipeline in the script")
	}

	return p.stack[0].pipeline, nil
}

// compile converts an input stream into the corresponding Expression.
func compile(program string) (s Pipeline, e error) {
	m := m3ql{scriptBuilder: scriptBuilder{
		macroDefs: make(map[string]*Function),
	}}

	m.Buffer = program
	m.Init()

	if err := m.Parse(); err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r == nil {
			return
		} else if rError, passthru := r.(runtime.Error); passthru {
			panic(fmt.Errorf("panic when compiling [ query = %s ]\n%v", program, rError.Error()))
		} else if _, castable := r.(error); castable {
			e = errors.NewParseErr(r.(error))
		} else {
			e = fmt.Errorf("unexpected panic in the M3QL compiler: %v", r)
		}
	}()

	// Since PEG cannot be stopped in the middle of execution, the compiler will instead
	// panic on errors and the handler above will convert panics to errors.
	m.Execute()

	// TODO(@kobolog): Collapse pipelines and return only the resulting pipeline.
	return m.build()
}

// ExtractFetchExpressions extracts all target queries from the given m3ql query
func ExtractFetchExpressions(s string) ([]string, error) {
	pipeline, err := compile(s)
	if err != nil {
		return nil, err
	}

	targets := make(map[string]interface{})
	findFetchExpressions(pipeline.(*optimizePipeline), targets)

	keys := make([]string, 0, len(targets))
	for k := range targets {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys, nil
}

func findFetchExpressions(op *optimizePipeline, targets map[string]interface{}) {
	for i := range op.segments {
		ps := op.segments[i].(*pipelineStep)

		for _, fa := range ps.FunctionArguments() {
			switch arg := fa.(type) {
			case *optimizePipeline:
				findFetchExpressions(arg, targets)
			}
		}

		switch exp := ps.expression.(type) {
		case *fetchExpression:
			targets[exp.query] = nil
		case *optimizePipeline:
			findFetchExpressions(exp, targets)
		}
	}
}
