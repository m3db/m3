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

package transformation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsUnaryTransform(t *testing.T) {
	inputs := []struct {
		typ      Type
		expected bool
	}{
		{typ: Absolute, expected: true},
		{typ: UnknownType, expected: false},
		{typ: PerSecond, expected: false},
		{typ: Type(10000), expected: false},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.typ.IsUnaryTransform())
	}
}

func TestIsBinaryTransform(t *testing.T) {
	inputs := []struct {
		typ      Type
		expected bool
	}{
		{typ: PerSecond, expected: true},
		{typ: UnknownType, expected: false},
		{typ: Absolute, expected: false},
		{typ: Type(10000), expected: false},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.typ.IsBinaryTransform())
	}
}

func TestUnaryTransform(t *testing.T) {
	inputs := []Type{
		Absolute,
	}

	for _, input := range inputs {
		tf, err := input.UnaryTransform()
		require.NoError(t, err)
		require.NotNil(t, tf)
	}
}

func TestUnaryTransformErrors(t *testing.T) {
	inputs := []Type{
		UnknownType,
		PerSecond,
		Type(10000),
	}

	for _, input := range inputs {
		tf, err := input.UnaryTransform()
		require.Error(t, err)
		require.Nil(t, tf)
	}
}

func TestMustUnaryTransform(t *testing.T) {
	inputs := []Type{
		Absolute,
	}

	for _, input := range inputs {
		var tf UnaryTransform
		require.NotPanics(t, func() { tf = input.MustUnaryTransform() })
		require.NotNil(t, tf)
	}
}

func TestMustUnaryTransformPanics(t *testing.T) {
	inputs := []Type{
		UnknownType,
		PerSecond,
		Type(10000),
	}

	for _, input := range inputs {
		var tf UnaryTransform
		require.Panics(t, func() { tf = input.MustUnaryTransform() })
		require.Nil(t, tf)
	}
}

func TestBinaryTransform(t *testing.T) {
	inputs := []Type{
		PerSecond,
	}

	for _, input := range inputs {
		tf, err := input.BinaryTransform()
		require.NoError(t, err)
		require.NotNil(t, tf)
	}
}

func TestBinaryTransformErrors(t *testing.T) {
	inputs := []Type{
		UnknownType,
		Absolute,
		Type(10000),
	}

	for _, input := range inputs {
		tf, err := input.BinaryTransform()
		require.Error(t, err)
		require.Nil(t, tf)
	}
}

func TestMustBinaryTransform(t *testing.T) {
	inputs := []Type{
		PerSecond,
	}

	for _, input := range inputs {
		var tf BinaryTransform
		require.NotPanics(t, func() { tf = input.MustBinaryTransform() })
		require.NotNil(t, tf)
	}
}

func TestMustBinaryTransformPanics(t *testing.T) {
	inputs := []Type{
		UnknownType,
		Absolute,
		Type(10000),
	}

	for _, input := range inputs {
		var tf BinaryTransform
		require.Panics(t, func() { tf = input.MustBinaryTransform() })
		require.Nil(t, tf)
	}
}

func TestTypeString(t *testing.T) {
	inputs := []struct {
		typ      Type
		expected string
	}{
		{typ: UnknownType, expected: "UnknownType"},
		{typ: Absolute, expected: "Absolute"},
		{typ: PerSecond, expected: "PerSecond"},
		{typ: Type(1000), expected: "Type(1000)"},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.typ.String())
	}
}
