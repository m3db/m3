// Copyright (c) 2018 Uber Technologies, Inc.
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

package models

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewNamespace(t *testing.T) {
	id := "name"
	fixture := testNamespaceView(id)
	expected := Namespace{
		ID:                id,
		ForRuleSetVersion: fixture.ForRuleSetVersion,
	}
	require.EqualValues(t, expected, NewNamespace(fixture))
}

func TestNewNamespaces(t *testing.T) {
	id1 := "name1"
	id2 := "name2"
	fixture := testNamespacesView(id1, id2)
	expected := Namespaces{
		Version: 1,
		Namespaces: []Namespace{
			{
				ID:                id1,
				ForRuleSetVersion: 1,
			},
			{
				ID:                id2,
				ForRuleSetVersion: 1,
			},
		},
	}
	require.EqualValues(t, expected, NewNamespaces(fixture))
}

func testNamespaceView(name string) *NamespaceView {
	return &NamespaceView{
		Name:              name,
		ForRuleSetVersion: 1,
	}
}

func testNamespacesView(namespaceNames ...string) *NamespacesView {
	namespaces := make([]*NamespaceView, len(namespaceNames))
	for i, name := range namespaceNames {
		namespaces[i] = testNamespaceView(name)
	}
	return &NamespacesView{
		Version:    1,
		Namespaces: namespaces,
	}
}
