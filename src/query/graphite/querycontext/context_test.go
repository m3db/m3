package querycontext

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
	engine := NewEngine(nil, nil)
	ctx := NewContext(ContextOptions{Start: time.Now(), End: time.Now(), Engine: engine})
	ctx.RegisterCloser(client)
	ctx.Close()
	client.AssertCalled(t, "foo")
}

func TestChildContextClose(t *testing.T) {
	client := &mockClient{}
	client.On("foo").Return()
	engine := NewEngine(nil, nil)
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
