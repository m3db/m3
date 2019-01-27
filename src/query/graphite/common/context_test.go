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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
