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
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/m3db/m3/src/query/graphite/common"
	xtest "github.com/m3db/m3/src/query/graphite/testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func f1(ctx *common.Context, a float64, b string, c bool) (string, error) {
	return fmt.Sprintf("%.3f %s %t", a, b, c), nil
}

func f2(ctx *common.Context, a ...string) ([]string, error) {
	sort.Strings(a)
	return a, nil
}

func f3(ctx *common.Context, values ...float64) (float64, error) {
	sum := float64(0)
	for _, n := range values {
		sum += n
	}

	return sum, nil
}

type testFunction struct {
	f              interface{}
	input          []interface{}
	expectedOutput interface{}
}

var testFunctions = []testFunction{
	{f1, []interface{}{635.6, "Hello", false}, "635.600 Hello false"},
	{f2, []interface{}{"b", "c", "a"}, []string{"a", "b", "c"}},
	{f3, []interface{}{10.0, 20.0, 30.0}, 60},
}

func TestFunctions(t *testing.T) {
	ctx := common.NewTestContext()
	defer ctx.Close()

	for _, tf := range testFunctions {
		f, err := buildFunction(tf.f)
		require.Nil(t, err, "could not build function %s", reflect.TypeOf(tf.f).Name())

		out, err := f.call(ctx, tf.input)
		require.Nil(t, err, "Could not call function %s", reflect.TypeOf(tf.f).Name())
		xtest.Equalish(t, tf.expectedOutput, out)
	}
}

func errorf(ctx *common.Context) ([]float64, error) {
	return nil, fmt.Errorf("this failed")
}

func TestFunctionReturningError(t *testing.T) {
	f, err := buildFunction(errorf)
	require.Nil(t, err)

	_, err = f.call(nil, nil)
	require.NotNil(t, err)
	assert.Equal(t, "this failed", err.Error())
}

type invalidFunction struct {
	name          string
	f             interface{}
	expectedError string
}

func badf1() (float64, error)                                               { return 0, nil }
func badf2(ctx *common.Context)                                             {}
func badf3(ctx *common.Context) (float32, error)                            { return 0, nil }
func badf4(ctx *common.Context) (string, string)                            { return "", "" }
func badf5(ctx *common.Context, n byte) (string, error)                     { return "", nil }
func badf6(ctx *common.Context, n float64) (byte, error)                    { return 0, nil }
func badf7(ctx *common.Context, foo, bar multiplePathSpecs) (string, error) { return "", nil }

func TestInvalidFunctions(t *testing.T) {
	invalidFunctions := []invalidFunction{
		{"badf1", badf1, "functions must take at least 1 argument"},
		{"badf2", badf2, "functions must return a value and an error"},
		{"badf3", badf3, "invalid return type float32"},
		{"badf4", badf4, "functions must return a value and an error"},
		{"badf5", badf5, "invalid arg 1: uint8 is not supported"},
		{"badf6", badf6, "invalid return type uint8"},
		{"24", 24, "not a function"},
		{"badf7", badf7, "invalid arg 1: multiplePathSpecs must be the last arg"},
	}

	for i, fn := range invalidFunctions {
		f, err := buildFunction(fn.f)
		require.NotNil(t, err, "invalid error for %s (%d)", fn.name, i)
		assert.Equal(t, fn.expectedError, err.Error(), "invalid error for %s (%d)", fn.name, i)
		assert.Nil(t, f)
	}
}
