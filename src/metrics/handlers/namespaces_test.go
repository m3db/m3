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

package handlers

import (
	"testing"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/stretchr/testify/require"
)

const (
	testNamespaceKey = "testKey"
)

var (
	testNamespaces = &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "fooNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        false,
					},
				},
			},
			&schema.Namespace{
				Name: "barNs",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 2,
						Tombstoned:        true,
					},
				},
			},
		},
	}

	badNamespaces = &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{Name: "fooNs", Snapshots: nil},
			&schema.Namespace{Name: "fooNs", Snapshots: nil},
		},
	}
)

func TestNamespace(t *testing.T) {
	res, err := Namespace(testNamespaces, "barNs")
	require.NoError(t, err)
	require.EqualValues(t, testNamespaces.Namespaces[1], res)
}

func TestNamespaceError(t *testing.T) {
	res, err := Namespace(badNamespaces, "blah")
	require.Error(t, err)
	require.Equal(t, err, kv.ErrNotFound)
	require.Nil(t, res)

	res, err = Namespace(badNamespaces, "fooNs")
	require.Error(t, err)
	require.Equal(t, err, errMultipleMatches)
	require.Nil(t, res)
}

func TestNamespaces(t *testing.T) {
	store := mem.NewStore()
	_, err := store.Set(testNamespaceKey, testNamespaces)
	require.NoError(t, err)
	_, s, err := Namespaces(store, testNamespaceKey)
	require.NoError(t, err)
	require.NotNil(t, s.Namespaces)
}

func TestNamespacesError(t *testing.T) {
	store := mem.NewStore()
	_, err := store.Set(testNamespaceKey, &schema.RollupRule{Uuid: "x"})
	require.NoError(t, err)
	_, s, err := Namespaces(store, testNamespaceKey)
	require.Error(t, err)
	require.Nil(t, s)
}

func TestValidateNamespace(t *testing.T) {
	store := mem.NewStore()
	_, err := store.Set(testNamespaceKey, testNamespaces)
	require.NoError(t, err)
	v, s, ns, err := ValidateNamespace(store, testNamespaceKey, "fooNs")
	require.Equal(t, v, 1)
	require.NoError(t, err)
	require.NotNil(t, s.Namespaces, nil)
	require.Equal(t, ns.Name, "fooNs")
}

func TestValidateNamespaceDNE(t *testing.T) {
	store := mem.NewStore()
	_, err := store.Set(testNamespaceKey, testNamespaces)
	require.NoError(t, err)
	_, _, _, err = ValidateNamespace(store, testNamespaceKey, "blah")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestValidateNamespaceTombstoned(t *testing.T) {
	store := mem.NewStore()
	_, err := store.Set(testNamespaceKey, testNamespaces)
	require.NoError(t, err)
	_, _, _, err = ValidateNamespace(store, testNamespaceKey, "barNs")
	require.Error(t, err)
	require.Contains(t, err.Error(), "tombstoned")
}
