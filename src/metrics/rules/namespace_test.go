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

package rules

import (
	"testing"

	"github.com/m3db/m3metrics/generated/proto/schema"

	"github.com/stretchr/testify/require"
)

func TestNewNamespaceSnapshotNilSchema(t *testing.T) {
	_, err := newNamespaceSnapshot(nil)
	require.Equal(t, err, errNilNamespaceSnapshotSchema)
}

func TestNewNamespaceSnapshotValidSchema(t *testing.T) {
	snapshot, err := newNamespaceSnapshot(&schema.NamespaceSnapshot{
		ForRulesetVersion: 123,
		Tombstoned:        true,
	})
	require.NoError(t, err)
	require.Equal(t, 123, snapshot.ForRuleSetVersion())
	require.Equal(t, true, snapshot.Tombstoned())
}

func TestNewNamespaceNilSchema(t *testing.T) {
	_, err := newNameSpace(nil)
	require.Equal(t, err, errNilNamespaceSchema)
}

func TestNewNamespaceValidSchema(t *testing.T) {
	ns, err := newNameSpace(&schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{
				ForRulesetVersion: 123,
				Tombstoned:        false,
			},
			&schema.NamespaceSnapshot{
				ForRulesetVersion: 456,
				Tombstoned:        true,
			},
		},
	})
	expected := []NamespaceSnapshot{
		{forRuleSetVersion: 123, tombstoned: false},
		{forRuleSetVersion: 456, tombstoned: true},
	}
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), ns.Name())
	require.Equal(t, expected, ns.Snapshots())
}

func TestNewNamespacesNilSchema(t *testing.T) {
	_, err := NewNamespaces(1, nil)
	require.Equal(t, errNilNamespacesSchema, err)
}

func TestNewNamespacesValidSchema(t *testing.T) {
	ns, err := NewNamespaces(1, &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 123,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 456,
						Tombstoned:        true,
					},
				},
			},
			&schema.Namespace{
				Name: "bar",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 789,
						Tombstoned:        false,
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion: 1000,
						Tombstoned:        true,
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, ns.Version())
	expected := []Namespace{
		{
			name: b("foo"),
			snapshots: []NamespaceSnapshot{
				{forRuleSetVersion: 123, tombstoned: false},
				{forRuleSetVersion: 456, tombstoned: true},
			},
		},
		{
			name: b("bar"),
			snapshots: []NamespaceSnapshot{
				{forRuleSetVersion: 789, tombstoned: false},
				{forRuleSetVersion: 1000, tombstoned: true},
			},
		},
	}
	require.Equal(t, expected, ns.Namespaces())
}
