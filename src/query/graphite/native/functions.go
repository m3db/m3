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

package native

import (
	"bytes"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/ts"
)

var (
	funcMut   sync.RWMutex
	functions = map[string]*Function{}
)

// list of graphite function name strings. (not whole list, update on-demand)
const (
	averageFnName        = "average"
	averageSeriesFnName  = "averageSeries"
	avgFnName            = "avg"
	countFnName          = "count"
	countSeriesFnName    = "countSeries"
	currentFnName        = "current"
	diffFnName           = "diff"
	diffSeriesFnName     = "diffSeries"
	emptyFnName          = ""
	lastFnName           = "last"
	lastSeriesFnName     = "lastSeries"
	maxFnName            = "max"
	maxSeriesFnName      = "maxSeries"
	medianFnName         = "median"
	medianSeriesFnName   = "medianSeries"
	minFnName            = "min"
	minSeriesFnName      = "minSeries"
	multiplyFnName       = "multiply"
	multiplySeriesFnName = "multiplySeries"
	powSeriesFnName      = "powSeries"
	rangeFnName          = "range"
	rangeOfFnName        = "rangeOf"
	rangeOfSeriesFnName  = "rangeOfSeries"
	stdevFnName          = "stdev"
	stddevFnName         = "stddev"
	stddevSeriesFnName   = "stddevSeries"
	sumFnName            = "sum"
	sumSeriesFnName      = "sumSeries"
	totalFnName          = "total"
)

// registerFunction is used to register a function under a specific name
func registerFunction(f interface{}) (*Function, error) {
	fn, err := buildFunction(f)
	if err != nil {
		return nil, err
	}

	funcMut.Lock()
	defer funcMut.Unlock()

	if functions[fn.name] != nil {
		return nil, fmt.Errorf("func %s already registered", fn.name)
	}
	functions[fn.name] = fn
	return fn, nil
}

// MustRegisterFunction registers a function, issuing a panic if the function cannot be registered
func MustRegisterFunction(f interface{}) *Function {
	if fn, err := registerFunction(f); err != nil {
		if name, nerr := functionName(f); nerr == nil {
			err = fmt.Errorf("could not register %s: %v", name, err)
		}
		panic(err)
	} else {
		return fn
	}
}

// registerAliasedFunction is used to register a function under an alias
func registerAliasedFunction(alias string, f interface{}) error {
	fname, err := functionName(f)
	if err != nil {
		return err
	}

	funcMut.Lock()
	defer funcMut.Unlock()

	if functions[alias] != nil {
		return fmt.Errorf("func %s already registered", alias)
	}

	fn := functions[fname]
	if fn == nil {
		return fmt.Errorf("target function %s not registered", fname)
	}

	functions[alias] = fn
	return nil
}

// MustRegisterAliasedFunction registers a function under an alias, issuing a panic if the function
// cannot be registered
func MustRegisterAliasedFunction(fname string, f interface{}) {
	if err := registerAliasedFunction(fname, f); err != nil {
		panic(err)
	}
}

// findFunction finds a function with the given name
func findFunction(name string) *Function {
	funcMut.RLock()
	defer funcMut.RUnlock()

	return functions[name]
}

// reflectTypeSet is a set of reflect.Type objects
type reflectTypeSet []reflect.Type

// contains checks whether the type set contains the given type
func (ts reflectTypeSet) contains(reflectType reflect.Type) bool {
	for i := range ts {
		if ts[i] == reflectType {
			return true
		}
	}

	return false
}

// singlePathSpec represents one wildcard pathspec argument that may fetch multiple time series
type singlePathSpec ts.SeriesList

// multiplePathSpecs represents a variadic number of wildcard pathspecs
type multiplePathSpecs ts.SeriesList

// genericInterface represents a value with an arbitrary type
type genericInterface interface{}

// contextShiftFunc generates a shifted context based on an input context
type contextShiftFunc func(*common.Context) *common.Context

// unaryTransformer takes in one series and returns a transformed series.
type unaryTransformer func(ts.SeriesList) (ts.SeriesList, error)

// binaryTransformer takes in two series and returns a transformed series.
type binaryTransformer func(ts.SeriesList, ts.SeriesList) (ts.SeriesList, error)

// unaryContextShifter contains a contextShiftFunc for generating shift contexts
// as well as a unaryTransformer for transforming one series to another.
type unaryContextShifter struct {
	ContextShiftFunc contextShiftFunc
	UnaryTransformer unaryTransformer
}

// binaryContextShifter contains a contextShiftFunc for generating shift contexts
// as well as a binaryTransformer for transforming one series to another.
type binaryContextShifter struct {
	ContextShiftFunc  contextShiftFunc
	BinaryTransformer binaryTransformer
}

var (
	contextPtrType              = reflect.TypeOf(&common.Context{})
	timeSeriesType              = reflect.TypeOf(&ts.Series{})
	timeSeriesListType          = reflect.SliceOf(timeSeriesType)
	seriesListType              = reflect.TypeOf(ts.NewSeriesList())
	unaryContextShifterPtrType  = reflect.TypeOf(&unaryContextShifter{})
	binaryContextShifterPtrType = reflect.TypeOf(&binaryContextShifter{})
	singlePathSpecType          = reflect.TypeOf(singlePathSpec{})
	multiplePathSpecsType       = reflect.TypeOf(multiplePathSpecs{})
	interfaceType               = reflect.TypeOf([]genericInterface{}).Elem()
	float64Type                 = reflect.TypeOf(float64(100))
	float64SliceType            = reflect.SliceOf(float64Type)
	intType                     = reflect.TypeOf(int(0))
	intSliceType                = reflect.SliceOf(intType)
	stringType                  = reflect.TypeOf("")
	stringSliceType             = reflect.SliceOf(stringType)
	boolType                    = reflect.TypeOf(false)
	boolSliceType               = reflect.SliceOf(boolType)
	errorType                   = reflect.TypeOf((*error)(nil)).Elem()
	genericInterfaceType        = reflect.TypeOf((*genericInterface)(nil)).Elem()
)

var (
	allowableTypes = reflectTypeSet{
		// these are for return types
		timeSeriesListType,
		unaryContextShifterPtrType,
		binaryContextShifterPtrType,
		seriesListType,
		singlePathSpecType,
		multiplePathSpecsType,
		interfaceType, // only for function parameters
		float64Type,
		float64SliceType,
		intType,
		intSliceType,
		stringType,
		stringSliceType,
		boolType,
		boolSliceType,
	}
)

var (
	errNonFunction   = errors.NewInvalidParamsError(errors.New("not a function"))
	errNeedsArgument = errors.NewInvalidParamsError(errors.New("functions must take at least 1 argument"))
	errNoContext     = errors.NewInvalidParamsError(errors.New("first argument must be a context"))
	errInvalidReturn = errors.NewInvalidParamsError(errors.New("functions must return a value and an error"))
)

// Function contains a function to invoke along with metadata about
// the function's argument and return type.
type Function struct {
	name     string
	f        reflect.Value
	in       []reflect.Type
	defaults map[uint8]interface{}
	out      reflect.Type
	variadic bool
}

// WithDefaultParams provides default parameters for functions
func (f *Function) WithDefaultParams(defaultParams map[uint8]interface{}) *Function {
	for index := range defaultParams {
		if int(index) <= 0 || int(index) > len(f.in) {
			panic(fmt.Sprintf("Default parameter #%d is out-of-range", index))
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

// validateContextShiftingFn validates if a function is a context shifting function.
func validateContextShiftingFn(in []reflect.Type) {
	// check that we have exactly *one* singlePathSpec parameter
	singlePathSpecParams := 0
	singlePathSpecIndex := -1
	for i, param := range in {
		if param == singlePathSpecType {
			singlePathSpecParams++
			singlePathSpecIndex = i
		}
	}
	if singlePathSpecParams != 1 {
		panic("A context-shifting function must have exactly one singlePathSpec parameter")
	}
	if singlePathSpecIndex != 0 {
		panic("A context-shifting function must have the singlePathSpec parameter as its first parameter")
	}
}

// buildFunction takes a reflection reference to a function and returns
// the function metadata
func buildFunction(f interface{}) (*Function, error) {
	fname, err := functionName(f)
	if err != nil {
		return nil, err
	}
	v := reflect.ValueOf(f)
	t := v.Type()
	if t.NumIn() == 0 {
		return nil, errNeedsArgument
	}

	if ctx := t.In(0); ctx != contextPtrType {
		return nil, errNoContext
	}

	var lastType reflect.Type
	in := make([]reflect.Type, 0, t.NumIn()-1)
	for i := 1; i < t.NumIn(); i++ {
		inArg := t.In(i)
		if !(allowableTypes.contains(inArg)) {
			return nil, fmt.Errorf("invalid arg %d: %s is not supported", i, inArg.Name())
		}
		if inArg == multiplePathSpecsType && i != t.NumIn()-1 {
			return nil, fmt.Errorf("invalid arg %d: multiplePathSpecs must be the last arg", i)
		}

		lastType = inArg
		in = append(in, inArg)
	}

	variadic := lastType == multiplePathSpecsType ||
		(lastType != nil &&
			lastType.Kind() == reflect.Slice &&
			lastType != singlePathSpecType)

	if variadic { // remove slice-ness of the variadic arg
		if lastType != multiplePathSpecsType {
			in[len(in)-1] = in[len(in)-1].Elem()
		}
	}

	if t.NumOut() != 2 {
		return nil, errInvalidReturn
	}

	out := t.Out(0)
	if !allowableTypes.contains(out) {
		return nil, fmt.Errorf("invalid return type %s", out.Name())
	} else if out == unaryContextShifterPtrType || out == binaryContextShifterPtrType {
		validateContextShiftingFn(in)
	}

	if ret2 := t.Out(1); ret2 != errorType {
		return nil, errInvalidReturn
	}

	return &Function{
		name:     fname,
		f:        v,
		in:       in,
		out:      out,
		variadic: variadic,
	}, nil
}

// call calls the function with non-reflected values
func (f *Function) call(ctx *common.Context, args []interface{}) (interface{}, error) {
	values := make([]reflect.Value, len(args))
	for i := range args {
		values[i] = reflect.ValueOf(args[i])
	}

	out, err := f.reflectCall(ctx, values)
	if err != nil {
		return nil, err
	}

	return out.Interface(), err
}

// reflectCall calls the function with reflected values, passing in the provided context and parameters
func (f *Function) reflectCall(ctx *common.Context, args []reflect.Value) (reflect.Value, error) {
	var instats []common.TraceStats

	in := make([]reflect.Value, 0, len(args)+1)
	in = append(in, reflect.ValueOf(ctx))
	for _, arg := range args {
		in = append(in, arg)
		if isTimeSeries(arg) {
			instats = append(instats, getStats(arg))
		}
	}

	// special case handling of multiplePathSpecs
	// NB(r): This code sucks, and it would be better if we just removed
	// multiplePathSpecs altogether and have the functions use real variadic
	// ts.SeriesList arguments so we don't have to autocollapse when calling here.
	// Notably singlePathSpec should also go and just replace usages with
	// barebones ts.SeriesList.  Then we can get rid of this code below and
	// the code the casts ts.SeriesList to the correct typealias of ts.SeriesList.
	if len(in) > len(f.in)+1 && len(f.in) > 0 && f.in[len(f.in)-1] == multiplePathSpecsType {
		var (
			series = make([]*ts.Series, 0, len(in))
			// Assume all sorted until proven otherwise
			sortedAll = true
			meta      = block.NewResultMetadata()
		)
		for i := len(f.in); i < len(in); i++ {
			v := in[i].Interface().(ts.SeriesList)

			// If any series lists are not sorted then the result
			// is not in deterministic sort order
			if sortedAll && !v.SortApplied {
				sortedAll = false
			}

			meta = meta.CombineMetadata(v.Metadata)
			series = append(series, v.Values...)
		}

		in[len(f.in)] = reflect.ValueOf(ts.SeriesList{
			Values: series,
			// Only consider the aggregation of all these series lists
			// sorted if and only if all originally had a sort applied
			SortApplied: sortedAll,
			Metadata:    meta,
		})

		in = in[:len(f.in)+1]
	}

	numTypes := len(f.in)
	numRequiredTypes := numTypes
	if f.variadic {
		// Variadic can avoid specifying the last arg.
		numRequiredTypes--
	}
	if len(in) < numRequiredTypes {
		err := fmt.Errorf("call args mismatch: expected at least %d, actual %d",
			len(f.in), len(in))
		return reflect.Value{}, err
	}

	// Cast to the expected typealias type of ts.SeriesList before calling
	for i, arg := range in {
		typeArg := arg.Type()
		if typeArg != seriesListType {
			continue
		}
		// NB(r): Poor form, ctx is not in f.in for no reason it seems...
		typeIdx := i - 1
		if i >= numTypes {
			typeIdx = numTypes - 1
		}
		l := arg.Interface().(ts.SeriesList)
		switch f.in[typeIdx] {
		case singlePathSpecType, genericInterfaceType:
			in[i] = reflect.ValueOf(singlePathSpec(l))
		case multiplePathSpecsType:
			in[i] = reflect.ValueOf(multiplePathSpecs(l))
		default:
			err := fmt.Errorf("cannot cast series to unexpected type: %s",
				f.in[typeIdx].String())
			return reflect.Value{}, err
		}
	}

	beginCall := time.Now()
	out := f.f.Call(in)
	outVal, errVal := out[0], out[1]
	var err error
	if !errVal.IsNil() {
		err = errVal.Interface().(error)
		return outVal, err
	}

	if ctx.TracingEnabled() {
		var outstats common.TraceStats
		if isTimeSeries(outVal) {
			outstats = getStats(outVal)
		}

		ctx.Trace(common.Trace{
			ActivityName: f.name,
			Inputs:       instats,
			Outputs:      outstats,
			Duration:     time.Since(beginCall),
		})
	}

	return outVal, nil
}

// A funcArg is an argument to a function that gets resolved at runtime
type funcArg interface {
	ArgumentASTNode
	Evaluate(ctx *common.Context) (reflect.Value, error)
	CompatibleWith(reflectType reflect.Type) bool
}

// A constFuncArg is a function argument that is a constant value
type constFuncArg struct {
	value reflect.Value
}

func newConstArg(i interface{}) funcArg { return constFuncArg{value: reflect.ValueOf(i)} }
func newBoolConst(b bool) funcArg       { return constFuncArg{value: reflect.ValueOf(b)} }
func newStringConst(s string) funcArg   { return constFuncArg{value: reflect.ValueOf(s)} }
func newFloat64Const(n float64) funcArg { return constFuncArg{value: reflect.ValueOf(n)} }
func newIntConst(n int) funcArg         { return constFuncArg{value: reflect.ValueOf(n)} }

func (c constFuncArg) Evaluate(ctx *common.Context) (reflect.Value, error) { return c.value, nil }
func (c constFuncArg) CompatibleWith(reflectType reflect.Type) bool {
	return c.value.Type() == reflectType || reflectType == interfaceType
}
func (c constFuncArg) String() string { return fmt.Sprintf("%v", c.value.Interface()) }

// A functionCall is an actual call to a function, with resolution for arguments
type functionCall struct {
	f  *Function
	in []funcArg
}

func (call *functionCall) Name() string {
	return call.f.name
}

func (call *functionCall) Arguments() []ArgumentASTNode {
	args := make([]ArgumentASTNode, len(call.in))
	for i, arg := range call.in {
		args[i] = arg
	}
	return args
}

// Evaluate evaluates the function call and returns the result as a reflect.Value
func (call *functionCall) Evaluate(ctx *common.Context) (reflect.Value, error) {
	values := make([]reflect.Value, len(call.in))
	for i, param := range call.in {
		if call.f.out == unaryContextShifterPtrType && call.f.in[i] == singlePathSpecType {
			values[i] = reflect.ValueOf(singlePathSpec{}) // fake parameter
			continue
		}
		value, err := param.Evaluate(ctx)
		if err != nil {
			return reflect.Value{}, err
		}
		values[i] = value
	}

	result, err := call.f.reflectCall(ctx, values)
	// if we have errors, or if we succeed and this is not a context-shifting function,
	// we return immediately
	if err != nil || call.f.out == seriesListType {
		return result, err
	}

	// context shifter ptr is nil, nothing to do here, return empty series.
	if result.IsNil() {
		return reflect.ValueOf(ts.NewSeriesList()), nil
	}

	contextShifter := result.Elem()
	ctxShiftingFn := contextShifter.Field(0)
	reflected := ctxShiftingFn.Call([]reflect.Value{reflect.ValueOf(ctx)})
	shiftedCtx := reflected[0].Interface().(*common.Context)
	shiftedSeries, err := call.in[0].Evaluate(shiftedCtx)
	if err != nil {
		return reflect.Value{}, err
	}
	transformerFn := contextShifter.Field(1)
	var ret []reflect.Value
	switch call.f.out {
	case unaryContextShifterPtrType:
		// unary function
		ret = transformerFn.Call([]reflect.Value{shiftedSeries})
	case binaryContextShifterPtrType:
		// binary function
		ret = transformerFn.Call([]reflect.Value{shiftedSeries, values[0]})
	default:
		return reflect.Value{}, fmt.Errorf("unknown context shift: %v", call.f.out)
	}
	if !ret[1].IsNil() {
		err = ret[1].Interface().(error)
	}
	return ret[0], err
}

// CompatibleWith checks whether the function call's return is compatible with the given reflection type
func (call *functionCall) CompatibleWith(reflectType reflect.Type) bool {
	if reflectType == interfaceType {
		return true
	}
	if call.f.out == unaryContextShifterPtrType || call.f.out == binaryContextShifterPtrType {
		return reflectType == singlePathSpecType || reflectType == multiplePathSpecsType
	}
	return call.f.out.Kind() == reflectType.Kind()
}

func (call *functionCall) String() string {
	var buf bytes.Buffer
	buf.WriteString(call.f.name)
	buf.WriteByte('(')
	for i := range call.in {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(call.in[i].String())
	}

	buf.WriteByte(')')
	return buf.String()
}

// isTimeSeries checks whether the given value contains a timeseries or
// timeseries list
func isTimeSeries(v reflect.Value) bool {
	return v.Type() == seriesListType
}

// getStats gets trace stats for the given timeseries argument
func getStats(v reflect.Value) common.TraceStats {
	if v.Type() == timeSeriesType {
		return common.TraceStats{NumSeries: 1}
	}

	l := v.Interface().(ts.SeriesList)
	return common.TraceStats{NumSeries: l.Len()}
}
