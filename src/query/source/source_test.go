// Copyright (c) 2021 Uber Technologies, Inc.
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

package source

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type testSource struct {
	name string
}

var testDeserialize = func(bytes []byte) (interface{}, error) {
	return testSource{string(bytes)}, nil
}

func TestSource(t *testing.T) {
	ctx, err := NewContext(context.Background(), []byte("foobar"), testDeserialize)
	require.NoError(t, err)

	typed, ok := FromContext(ctx)
	require.True(t, ok)
	require.Equal(t, testSource{"foobar"}, typed.(testSource))

	raw, ok := RawFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, []byte("foobar"), raw)
}

func TestNoTypedSource(t *testing.T) {
	ctx, err := NewContext(context.Background(), []byte("foobar"), nil)
	require.NoError(t, err)

	typed, ok := FromContext(ctx)
	require.False(t, ok)
	require.Nil(t, typed)

	raw, ok := RawFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, []byte("foobar"), raw)
}

func TestNoSource(t *testing.T) {
	typed, ok := FromContext(context.Background())
	require.False(t, ok)
	require.Nil(t, typed)

	raw, ok := RawFromContext(context.Background())
	require.False(t, ok)
	require.Nil(t, raw)
}

func TestNilSource(t *testing.T) {
	ctx, err := NewContext(context.Background(), nil, func(bytes []byte) (interface{}, error) {
		return nil, errors.New("should never happen")
	})
	require.NoError(t, err)

	_, ok := FromContext(ctx)
	require.False(t, ok)

	raw, ok := RawFromContext(ctx)
	require.False(t, ok)
	require.Nil(t, raw)
}

func TestFromContextErr(t *testing.T) {
	ctx, err := NewContext(context.Background(), []byte("foobar"), func(bytes []byte) (interface{}, error) {
		return nil, errors.New("boom")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	require.Nil(t, ctx)
}
