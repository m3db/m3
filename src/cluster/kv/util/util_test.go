// Copyright (c) 2017 Uber Technologies, Inc.
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

package util

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"

	"github.com/stretchr/testify/require"
)

func TestBoolFromValue(t *testing.T) {
	defaultValue := true

	tests := []struct {
		input       kv.Value
		expectedErr bool
		expectedVal bool
	}{
		{
			input:       mem.NewValue(0, &commonpb.BoolProto{Value: true}),
			expectedErr: false,
			expectedVal: true,
		},
		{
			input:       mem.NewValue(0, &commonpb.BoolProto{Value: false}),
			expectedErr: false,
			expectedVal: false,
		},
		{
			input:       nil,
			expectedErr: false,
			expectedVal: defaultValue,
		},
		{
			input:       mem.NewValue(0, &commonpb.Float64Proto{Value: 123}),
			expectedErr: true,
		},
	}

	for _, test := range tests {
		v, err := BoolFromValue(test.input, "key", defaultValue, nil)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, test.expectedVal, v)
	}

	// Invalid updates should return an error.
	opts := NewOptions().SetValidateFn(testValidateBoolFn)
	_, err := BoolFromValue(
		mem.NewValue(0, &commonpb.BoolProto{Value: false}), "key", defaultValue, opts,
	)
	require.Error(t, err)
}

func TestFloat64FromValue(t *testing.T) {
	defaultValue := 3.7

	tests := []struct {
		input       kv.Value
		expectedErr bool
		expectedVal float64
	}{
		{
			input:       mem.NewValue(0, &commonpb.Float64Proto{Value: 0}),
			expectedErr: false,
			expectedVal: 0,
		},
		{
			input:       mem.NewValue(0, &commonpb.Float64Proto{Value: 13.2}),
			expectedErr: false,
			expectedVal: 13.2,
		},
		{
			input:       nil,
			expectedErr: false,
			expectedVal: defaultValue,
		},
		{
			input:       mem.NewValue(0, &commonpb.Int64Proto{Value: 123}),
			expectedErr: true,
		},
	}

	for _, test := range tests {
		v, err := Float64FromValue(test.input, "key", defaultValue, nil)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, test.expectedVal, v)
	}

	// Invalid updates should return an error.
	opts := NewOptions().SetValidateFn(testValidateBoolFn)
	_, err := Float64FromValue(
		mem.NewValue(0, &commonpb.Float64Proto{Value: 1.24}), "key", defaultValue, opts,
	)
	require.Error(t, err)
}

func TestInt64FromValue(t *testing.T) {
	var defaultValue int64 = 5

	tests := []struct {
		input       kv.Value
		expectedErr bool
		expectedVal int64
	}{
		{
			input:       mem.NewValue(0, &commonpb.Int64Proto{Value: 0}),
			expectedErr: false,
			expectedVal: 0,
		},
		{
			input:       mem.NewValue(0, &commonpb.Int64Proto{Value: 13}),
			expectedErr: false,
			expectedVal: 13,
		},
		{
			input:       nil,
			expectedErr: false,
			expectedVal: defaultValue,
		},
		{
			input:       mem.NewValue(0, &commonpb.Float64Proto{Value: 1.23}),
			expectedErr: true,
		},
	}

	for _, test := range tests {
		v, err := Int64FromValue(test.input, "key", defaultValue, nil)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, test.expectedVal, v)
	}

	// Invalid updates should return an error.
	opts := NewOptions().SetValidateFn(testValidateInt64Fn)
	_, err := Int64FromValue(
		mem.NewValue(0, &commonpb.Int64Proto{Value: 22}), "key", defaultValue, opts,
	)
	require.Error(t, err)
}

func TestTimeFromValue(t *testing.T) {
	var (
		zero         = time.Time{}
		defaultValue = time.Now()
		customValue  = defaultValue.Add(time.Minute)
	)

	tests := []struct {
		input       kv.Value
		expectedErr bool
		expectedVal time.Time
	}{
		{
			input:       mem.NewValue(0, &commonpb.Int64Proto{Value: zero.Unix()}),
			expectedErr: false,
			expectedVal: zero,
		},
		{
			input:       mem.NewValue(0, &commonpb.Int64Proto{Value: customValue.Unix()}),
			expectedErr: false,
			expectedVal: customValue,
		},
		{
			input:       nil,
			expectedErr: false,
			expectedVal: defaultValue,
		},
		{
			input:       mem.NewValue(0, &commonpb.Float64Proto{Value: 1.23}),
			expectedErr: true,
		},
	}

	for _, test := range tests {
		v, err := TimeFromValue(test.input, "key", defaultValue, nil)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, test.expectedVal.Unix(), v.Unix())
	}

	// Invalid updates should return an error.
	opts := NewOptions().SetValidateFn(testValidateTimeFn)
	_, err := TimeFromValue(
		mem.NewValue(0, &commonpb.Int64Proto{Value: testNow.Add(time.Hour).Unix()}),
		"key",
		defaultValue,
		opts,
	)
	require.Error(t, err)
}

func TestStringFromValue(t *testing.T) {
	defaultValue := "bcd"

	tests := []struct {
		input       kv.Value
		expectedErr bool
		expectedVal string
	}{
		{
			input:       mem.NewValue(0, &commonpb.StringProto{Value: ""}),
			expectedErr: false,
			expectedVal: "",
		},
		{
			input:       mem.NewValue(0, &commonpb.StringProto{Value: "foo"}),
			expectedErr: false,
			expectedVal: "foo",
		},
		{
			input:       nil,
			expectedErr: false,
			expectedVal: defaultValue,
		},
		{
			input:       mem.NewValue(0, &commonpb.Float64Proto{Value: 1.23}),
			expectedErr: true,
		},
	}

	for _, test := range tests {
		v, err := StringFromValue(test.input, "key", defaultValue, nil)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, test.expectedVal, v)
	}

	// Invalid updates should return an error.
	opts := NewOptions().SetValidateFn(testValidateStringFn)
	_, err := StringFromValue(
		mem.NewValue(0, &commonpb.StringProto{Value: "abc"}), "key", defaultValue, opts,
	)
	require.Error(t, err)
}

func TestStringArrayFromValue(t *testing.T) {
	defaultValue := []string{"a", "b"}

	tests := []struct {
		input       kv.Value
		expectedErr bool
		expectedVal []string
	}{
		{
			input:       mem.NewValue(0, &commonpb.StringArrayProto{Values: nil}),
			expectedErr: false,
			expectedVal: nil,
		},
		{
			input:       mem.NewValue(0, &commonpb.StringArrayProto{Values: []string{"foo", "bar"}}),
			expectedErr: false,
			expectedVal: []string{"foo", "bar"},
		},
		{
			input:       nil,
			expectedErr: false,
			expectedVal: defaultValue,
		},
		{
			input:       mem.NewValue(0, &commonpb.Float64Proto{Value: 1.23}),
			expectedErr: true,
		},
	}

	for _, test := range tests {
		v, err := StringArrayFromValue(test.input, "key", defaultValue, nil)
		if test.expectedErr {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, test.expectedVal, v)
	}

	// Invalid updates should return an error.
	opts := NewOptions().SetValidateFn(testValidateStringArrayFn)
	_, err := StringArrayFromValue(
		mem.NewValue(0, &commonpb.StringArrayProto{Values: []string{"abc"}}), "key", defaultValue, opts,
	)
	require.Error(t, err)
}
