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

package common

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type closerFunc func() error

func (f closerFunc) Close() error {
	return f()
}

func TestChildContext(t *testing.T) {
	var (
		ctx      = NewTestContext()
		childCtx = ctx.NewChildContext(NewChildContextOptions())
		called   = false
	)

	ctx.RegisterCloser(closerFunc(func() error {
		called = true
		return nil
	}))

	childCtx.Close()
	assert.False(t, called, "child context has closed the context root")

	ctx.Close()
	assert.True(t, called, "parent context hasn't closed the context root")
}

type mockClient struct {
	mock.Mock
}

func (m *mockClient) foo() {
	m.Called()
}

func (m *mockClient) Close() error {
	m.foo()
	return nil
}

func TestContextClose(t *testing.T) {
	client := &mockClient{}
	client.On("foo").Return()
	engine := NewEngine(nil)
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now(), Engine: engine})
	ctx.RegisterCloser(client)
	ctx.Close()
	client.AssertCalled(t, "foo")
}

func TestChildContextClose(t *testing.T) {
	client := &mockClient{}
	client.On("foo").Return()
	engine := NewEngine(nil)
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now(), Engine: engine})
	childContext := ctx.NewChildContext(NewChildContextOptions())
	childContext.RegisterCloser(client)
	childContext.Close()
	client.AssertNotCalled(t, "foo")
	ctx.Close()
	client.AssertCalled(t, "foo")
}

func TestChildContextIsCancelledDefersToParentIsCancelled(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())
	assert.Equal(t, false, ctx.IsCancelled())
	assert.Equal(t, false, child.IsCancelled())

	ctx.Cancel()

	assert.Equal(t, true, ctx.IsCancelled())
	assert.Equal(t, true, child.IsCancelled())
}

func TestChildCancelDoesNotPropagateUp(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())
	assert.Equal(t, false, ctx.IsCancelled())
	assert.Equal(t, false, child.IsCancelled())

	child.Cancel()

	assert.Equal(t, true, child.IsCancelled())
	assert.Equal(t, false, ctx.IsCancelled())
}

func TestChildSetsParentStorage(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())

	k, v := "a", "b"
	child.Set(k, v)
	assert.Equal(t, v, child.Get(k))
	assert.Equal(t, v, ctx.Get(k))
}

func TestChildGetsParentStorage(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())

	k, v := "a", "b"
	ctx.Set(k, v)
	assert.Equal(t, v, child.Get(k))
	assert.Equal(t, v, ctx.Get(k))
}

func TestChildPropagatesWarningsToParent(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())
	e := fmt.Errorf("err")
	child.AddWarning(e)

	assert.Contains(t, ctx.Warnings(), e, "warning not added in context")
	assert.Contains(t, child.Warnings(), e, "warning not added in child")

	child.ClearWarnings()
	assert.Equal(t, 0, len(child.Warnings()))
	assert.Equal(t, 0, len(ctx.Warnings()))
}

func TestParentPropagatesWarningsToChild(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())
	e := fmt.Errorf("err")
	ctx.AddWarning(e)

	assert.Contains(t, ctx.Warnings(), e, "warning not added in context")
	assert.Contains(t, child.Warnings(), e, "warning not added in child")

	ctx.ClearWarnings()
	assert.Equal(t, 0, len(child.Warnings()))
	assert.Equal(t, 0, len(ctx.Warnings()))
}

func TestChildPropagatesBreakdownToParent(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())

	require.Equal(t, 0, len(ctx.FetchBreakdowns()))
	require.Equal(t, 0, len(child.FetchBreakdowns()))

	breakdown := FetchBreakdown{
		NumSeries:   1,
		CompletedAt: time.Now(),
		LocalOnly:   true,
	}
	child.AddFetchBreakdown(breakdown)

	require.Equal(t, 1, len(ctx.FetchBreakdowns()))
	require.Equal(t, 1, len(child.FetchBreakdowns()))
	assert.True(t, breakdown.Equal(ctx.FetchBreakdowns()[0]))
	assert.True(t, breakdown.Equal(child.FetchBreakdowns()[0]))
}

func TestParentDoesNotPropagateBreakdownToChild(t *testing.T) {
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now()})
	child := ctx.NewChildContext(NewChildContextOptions())

	require.Equal(t, 0, len(ctx.FetchBreakdowns()))
	require.Equal(t, 0, len(child.FetchBreakdowns()))

	breakdown := FetchBreakdown{
		NumSeries:   1,
		CompletedAt: time.Now(),
		LocalOnly:   true,
	}
	ctx.AddFetchBreakdown(breakdown)

	require.Equal(t, 1, len(ctx.FetchBreakdowns()))
	require.Equal(t, 0, len(child.FetchBreakdowns()))
	assert.True(t, breakdown.Equal(ctx.FetchBreakdowns()[0]))
}
