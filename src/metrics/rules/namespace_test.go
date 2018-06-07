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

	schema "github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/rules/models"

	"github.com/stretchr/testify/require"
)

func TestNewNamespaceSnapshotFromNilSchema(t *testing.T) {
	_, err := newNamespaceSnapshot(nil)
	require.Equal(t, err, errNilNamespaceSnapshotSchema)
}

func TestNewNamespaceSnapshotFromValidSchema(t *testing.T) {
	snapshot, err := newNamespaceSnapshot(&schema.NamespaceSnapshot{
		ForRulesetVersion:  123,
		Tombstoned:         true,
		LastUpdatedAtNanos: 456,
		LastUpdatedBy:      "someone",
	})
	require.NoError(t, err)
	require.Equal(t, 123, snapshot.ForRuleSetVersion())
	require.Equal(t, true, snapshot.Tombstoned())
	require.Equal(t, int64(456), snapshot.LastUpdatedAtNanos())
	require.Equal(t, "someone", snapshot.LastUpdatedBy())
}

func TestNamespaceSnapshotToSchema(t *testing.T) {
	snapshot := NamespaceSnapshot{
		forRuleSetVersion:  123,
		tombstoned:         true,
		lastUpdatedAtNanos: 456,
		lastUpdatedBy:      "someone",
	}
	schema := snapshot.Schema()
	require.Equal(t, int32(123), schema.ForRulesetVersion)
	require.Equal(t, true, schema.Tombstoned)
	require.Equal(t, int64(456), schema.LastUpdatedAtNanos)
	require.Equal(t, "someone", schema.LastUpdatedBy)
}

func TestNamespaceSnapshotRoundTrip(t *testing.T) {
	schema := &schema.NamespaceSnapshot{
		ForRulesetVersion:  123,
		Tombstoned:         true,
		LastUpdatedAtNanos: 456,
		LastUpdatedBy:      "someone",
	}
	snapshot, err := newNamespaceSnapshot(schema)
	require.NoError(t, err)
	res := snapshot.Schema()
	require.Equal(t, schema, res)
}

func TestNamespaceView(t *testing.T) {
	ns := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  123,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
			{
				forRuleSetVersion:  456,
				tombstoned:         true,
				lastUpdatedAtNanos: 7890,
				lastUpdatedBy:      "someone else",
			},
		},
	}

	expected := []*models.NamespaceView{
		&models.NamespaceView{
			Name:               "foo",
			ForRuleSetVersion:  123,
			Tombstoned:         false,
			LastUpdatedAtNanos: 456,
			LastUpdatedBy:      "someone",
		},
		&models.NamespaceView{
			Name:               "foo",
			ForRuleSetVersion:  456,
			Tombstoned:         true,
			LastUpdatedAtNanos: 7890,
			LastUpdatedBy:      "someone else",
		},
	}
	for i := range ns.snapshots {
		res, err := ns.NamespaceView(i)
		require.NoError(t, err)
		require.Equal(t, expected[i], res)
	}
}

func TestNamespaceViewError(t *testing.T) {
	n := Namespace{
		name: b("test"),
		snapshots: []NamespaceSnapshot{
			NamespaceSnapshot{forRuleSetVersion: 3, tombstoned: false},
			NamespaceSnapshot{forRuleSetVersion: 4, tombstoned: true},
		},
	}

	badIdx := []int{-2, 2, 30}
	for _, i := range badIdx {
		actual, err := n.NamespaceView(i)
		require.Error(t, err)
		require.Nil(t, actual)
	}
}

func TestNamespaceClone(t *testing.T) {
	ns := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  123,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
			{
				forRuleSetVersion:  456,
				tombstoned:         true,
				lastUpdatedAtNanos: 7890,
				lastUpdatedBy:      "someone else",
			},
		},
	}

	// Assert a clone looks the same as the original.
	nsClone := ns.clone()
	require.Equal(t, ns, nsClone)

	// Assert changing the clone does not change the original.
	nsClone.snapshots[0].forRuleSetVersion = 2934
	require.NotEqual(t, ns, nsClone)
}

func TestNewNamespaceFromNilSchema(t *testing.T) {
	_, err := newNamespace(nil)
	require.Equal(t, err, errNilNamespaceSchema)
}

func TestNewNamespaceFromValidSchema(t *testing.T) {
	ns, err := newNamespace(&schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{
				ForRulesetVersion:  123,
				Tombstoned:         false,
				LastUpdatedAtNanos: 456,
				LastUpdatedBy:      "someone",
			},
			&schema.NamespaceSnapshot{
				ForRulesetVersion:  456,
				Tombstoned:         true,
				LastUpdatedAtNanos: 7890,
				LastUpdatedBy:      "someone else",
			},
		},
	})
	expected := []NamespaceSnapshot{
		{
			forRuleSetVersion:  123,
			tombstoned:         false,
			lastUpdatedAtNanos: 456,
			lastUpdatedBy:      "someone",
		},
		{
			forRuleSetVersion:  456,
			tombstoned:         true,
			lastUpdatedAtNanos: 7890,
			lastUpdatedBy:      "someone else",
		},
	}
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), ns.Name())
	require.Equal(t, expected, ns.Snapshots())
}

func TestNamespaceToSchema(t *testing.T) {
	ns := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  123,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
			{
				forRuleSetVersion:  456,
				tombstoned:         true,
				lastUpdatedAtNanos: 7890,
				lastUpdatedBy:      "someone else",
			},
		},
	}
	res, err := ns.Schema()
	require.NoError(t, err)

	expected := &schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{
				ForRulesetVersion:  123,
				Tombstoned:         false,
				LastUpdatedAtNanos: 456,
				LastUpdatedBy:      "someone",
			},
			&schema.NamespaceSnapshot{
				ForRulesetVersion:  456,
				Tombstoned:         true,
				LastUpdatedAtNanos: 7890,
				LastUpdatedBy:      "someone else",
			},
		},
	}
	require.Equal(t, expected, res)
}

func TestNamespaceToSchemaNoSnapshots(t *testing.T) {
	badNs := Namespace{
		name: []byte("foo"),
	}
	res, err := badNs.Schema()
	require.Equal(t, errNilNamespaceSnapshot, err)
	require.Nil(t, res)
}

func TestNamespaceRoundTrip(t *testing.T) {
	testNs := &schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{
				ForRulesetVersion:  123,
				Tombstoned:         false,
				LastUpdatedAtNanos: 456,
				LastUpdatedBy:      "someone",
			},
			&schema.NamespaceSnapshot{
				ForRulesetVersion:  456,
				Tombstoned:         true,
				LastUpdatedAtNanos: 7890,
				LastUpdatedBy:      "someone else",
			},
		},
	}

	ns, err := newNamespace(testNs)
	require.NoError(t, err)

	res, err := ns.Schema()
	require.NoError(t, err)

	require.Equal(t, testNs, res)
}

func TestNamespaceMarkTombstoned(t *testing.T) {
	ns := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  1,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
		},
	}
	meta := UpdateMetadata{updatedAtNanos: 789, updatedBy: "someone else"}
	require.NoError(t, ns.markTombstoned(4, meta))
	require.True(t, ns.Tombstoned())

	expected := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  1,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
			{
				forRuleSetVersion:  4,
				tombstoned:         true,
				lastUpdatedAtNanos: 789,
				lastUpdatedBy:      "someone else",
			},
		},
	}
	require.Equal(t, expected, ns)
}

func TestNamespaceMarkTombstonedAlreadyTombstoned(t *testing.T) {
	ns := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  1,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
			{
				forRuleSetVersion:  4,
				tombstoned:         true,
				lastUpdatedAtNanos: 789,
				lastUpdatedBy:      "someone else",
			},
		},
	}
	require.Equal(t, errNamespaceAlreadyTombstoned, ns.markTombstoned(4, UpdateMetadata{}))
}

func TestNamespaceRevive(t *testing.T) {
	ns := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  1,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
			{
				forRuleSetVersion:  4,
				tombstoned:         true,
				lastUpdatedAtNanos: 789,
				lastUpdatedBy:      "someone else",
			},
		},
	}
	meta := UpdateMetadata{updatedAtNanos: 2378, updatedBy: "john"}
	require.NoError(t, ns.revive(meta))

	expected := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  1,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
			{
				forRuleSetVersion:  4,
				tombstoned:         true,
				lastUpdatedAtNanos: 789,
				lastUpdatedBy:      "someone else",
			},
			{
				forRuleSetVersion:  5,
				tombstoned:         false,
				lastUpdatedAtNanos: 2378,
				lastUpdatedBy:      "john",
			},
		},
	}
	require.Equal(t, expected, ns)
}

func TestNamespaceReviveNotTombstoned(t *testing.T) {
	ns := Namespace{
		name: b("foo"),
		snapshots: []NamespaceSnapshot{
			{
				forRuleSetVersion:  1,
				tombstoned:         false,
				lastUpdatedAtNanos: 456,
				lastUpdatedBy:      "someone",
			},
		},
	}
	require.Equal(t, errNamespaceNotTombstoned, ns.revive(UpdateMetadata{}))
}

func TestNamespaceReviveNoSnapshots(t *testing.T) {
	ns := Namespace{
		name:      b("foo"),
		snapshots: []NamespaceSnapshot{},
	}
	require.Equal(t, errNoNamespaceSnapshots, ns.revive(UpdateMetadata{}))
}

func TestNamespaceTombstoned(t *testing.T) {
	inputs := []struct {
		ns       Namespace
		expected bool
	}{
		{
			ns:       Namespace{name: b("foo")},
			expected: true,
		},
		{
			ns: Namespace{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  1,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
				},
			},
			expected: false,
		},
		{
			ns: Namespace{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  1,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  4,
						tombstoned:         true,
						lastUpdatedAtNanos: 789,
						lastUpdatedBy:      "someone else",
					},
				},
			},
			expected: true,
		},
	}

	for _, input := range inputs {
		require.Equal(t, input.expected, input.ns.Tombstoned())
	}
}

func TestNamespacesView(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  456,
						tombstoned:         true,
						lastUpdatedAtNanos: 7890,
						lastUpdatedBy:      "someone else",
					},
				},
			},
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}

	expected := &models.NamespacesView{
		Version: 1,
		Namespaces: []*models.NamespaceView{
			&models.NamespaceView{
				Name:               "foo",
				ForRuleSetVersion:  456,
				Tombstoned:         true,
				LastUpdatedAtNanos: 7890,
				LastUpdatedBy:      "someone else",
			},
			&models.NamespaceView{
				Name:               "bar",
				ForRuleSetVersion:  789,
				Tombstoned:         false,
				LastUpdatedAtNanos: 12345,
				LastUpdatedBy:      "john",
			},
		},
	}

	actual, err := nss.NamespacesView()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestNamespacesClone(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  456,
						tombstoned:         true,
						lastUpdatedAtNanos: 7890,
						lastUpdatedBy:      "someone else",
					},
				},
			},
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}

	// Assert clone looks the same as the original.
	nssClone := nss.Clone()
	require.Equal(t, nss, nssClone)
	require.False(t, &nss.namespaces[0] == &nssClone.namespaces[0])

	// Assert changing the clone does not affect the original.
	nssClone.Namespaces()[0].Snapshots()[0].forRuleSetVersion = 384
	require.NotEqual(t, nss, nssClone)
}

func TestNewNamespacesFromNilSchema(t *testing.T) {
	_, err := NewNamespaces(1, nil)
	require.Equal(t, errNilNamespacesSchema, err)
}

func TestNewNamespacesFromValidSchema(t *testing.T) {
	ns, err := NewNamespaces(1, &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  123,
						Tombstoned:         false,
						LastUpdatedAtNanos: 456,
						LastUpdatedBy:      "someone",
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  456,
						Tombstoned:         true,
						LastUpdatedAtNanos: 7890,
						LastUpdatedBy:      "someone else",
					},
				},
			},
			&schema.Namespace{
				Name: "bar",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  789,
						Tombstoned:         false,
						LastUpdatedAtNanos: 12345,
						LastUpdatedBy:      "john",
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  1000,
						Tombstoned:         true,
						LastUpdatedAtNanos: 67890,
						LastUpdatedBy:      "joe",
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
				{
					forRuleSetVersion:  123,
					tombstoned:         false,
					lastUpdatedAtNanos: 456,
					lastUpdatedBy:      "someone",
				},
				{
					forRuleSetVersion:  456,
					tombstoned:         true,
					lastUpdatedAtNanos: 7890,
					lastUpdatedBy:      "someone else",
				},
			},
		},
		{
			name: b("bar"),
			snapshots: []NamespaceSnapshot{
				{
					forRuleSetVersion:  789,
					tombstoned:         false,
					lastUpdatedAtNanos: 12345,
					lastUpdatedBy:      "john",
				},
				{
					forRuleSetVersion:  1000,
					tombstoned:         true,
					lastUpdatedAtNanos: 67890,
					lastUpdatedBy:      "joe",
				},
			},
		},
	}
	require.Equal(t, expected, ns.Namespaces())
}

func TestNamespacesRoundTrip(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  123,
						Tombstoned:         false,
						LastUpdatedAtNanos: 456,
						LastUpdatedBy:      "someone",
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  456,
						Tombstoned:         true,
						LastUpdatedAtNanos: 7890,
						LastUpdatedBy:      "someone else",
					},
				},
			},
			&schema.Namespace{
				Name: "foo2",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  789,
						Tombstoned:         false,
						LastUpdatedAtNanos: 12345,
						LastUpdatedBy:      "john",
					},
					&schema.NamespaceSnapshot{
						ForRulesetVersion:  1000,
						Tombstoned:         true,
						LastUpdatedAtNanos: 67890,
						LastUpdatedBy:      "joe",
					},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	res, err := nss.Schema()
	require.NoError(t, err)
	require.Equal(t, testNss, res)
}

func TestNamespacesNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  456,
						tombstoned:         true,
						lastUpdatedAtNanos: 7890,
						lastUpdatedBy:      "someone else",
					},
				},
			},
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}

	inputs := []string{"foo", "bar"}
	for _, input := range inputs {
		ns, err := nss.Namespace(input)
		require.NoError(t, err)
		require.Equal(t, string(ns.Name()), input)
	}
}

func TestNamespacesNamespaceNotFound(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}
	_, err := nss.Namespace("foo")
	require.Equal(t, errNamespaceNotFound, err)
}

func TestNamespacesNamespaceMultipleMatches(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}
	_, err := nss.Namespace("bar")
	require.Equal(t, errMultipleNamespaceMatches, err)
}

func TestNamespacesAddNewNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  456,
						tombstoned:         true,
						lastUpdatedAtNanos: 7890,
						lastUpdatedBy:      "someone else",
					},
				},
			},
		},
	}
	meta := UpdateMetadata{updatedAtNanos: 12345, updatedBy: "john"}
	revived, err := nss.AddNamespace("bar", meta)
	require.NoError(t, err)
	require.False(t, revived)

	expected := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  456,
						tombstoned:         true,
						lastUpdatedAtNanos: 7890,
						lastUpdatedBy:      "someone else",
					},
				},
			},
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  1,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}
	require.Equal(t, expected, nss)
}

func TestNamespacesAddTombstonedNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  456,
						tombstoned:         true,
						lastUpdatedAtNanos: 7890,
						lastUpdatedBy:      "someone else",
					},
				},
			},
		},
	}

	meta := UpdateMetadata{updatedAtNanos: 12345, updatedBy: "john"}
	revived, err := nss.AddNamespace("foo", meta)
	require.NoError(t, err)
	require.True(t, revived)

	expected := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  456,
						tombstoned:         true,
						lastUpdatedAtNanos: 7890,
						lastUpdatedBy:      "someone else",
					},
					{
						forRuleSetVersion:  457,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}
	require.Equal(t, expected, nss)
}

func TestNamespacesAddLiveNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
				},
			},
		},
	}
	_, err := nss.AddNamespace("foo", UpdateMetadata{})
	require.Error(t, err)
}

func TestNamespacesDeleteNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
				},
			},
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}

	meta := UpdateMetadata{updatedAtNanos: 1000, updatedBy: "someone else"}
	require.NoError(t, nss.DeleteNamespace("foo", 200, meta))

	expected := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  201,
						tombstoned:         true,
						lastUpdatedAtNanos: 1000,
						lastUpdatedBy:      "someone else",
					},
				},
			},
			{
				name: b("bar"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  789,
						tombstoned:         false,
						lastUpdatedAtNanos: 12345,
						lastUpdatedBy:      "john",
					},
				},
			},
		},
	}
	require.Equal(t, expected, nss)
}

func TestNamespacesDeleteMissingNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
				},
			},
		},
	}
	require.Error(t, nss.DeleteNamespace("bar", 300, UpdateMetadata{}))
}

func TestNamespacesDeleteTombstonedNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  123,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
					{
						forRuleSetVersion:  201,
						tombstoned:         true,
						lastUpdatedAtNanos: 1000,
						lastUpdatedBy:      "someone else",
					},
				},
			},
		},
	}
	require.Error(t, nss.DeleteNamespace("foo", 300, UpdateMetadata{}))
}

func TestNamespacesDeleteAndReviveNamespace(t *testing.T) {
	nss := Namespaces{
		version: 1,
		namespaces: []Namespace{
			{
				name: b("foo"),
				snapshots: []NamespaceSnapshot{
					{
						forRuleSetVersion:  1,
						tombstoned:         false,
						lastUpdatedAtNanos: 456,
						lastUpdatedBy:      "someone",
					},
				},
			},
		},
	}

	ns, err := nss.Namespace("foo")
	require.NoError(t, err)
	require.False(t, ns.Tombstoned())
	require.Equal(t, len(ns.Snapshots()), 1)
	lastSnapshot := ns.Snapshots()[0]
	require.Equal(t, 1, lastSnapshot.ForRuleSetVersion())
	require.False(t, lastSnapshot.Tombstoned())
	require.Equal(t, int64(456), lastSnapshot.LastUpdatedAtNanos())
	require.Equal(t, "someone", lastSnapshot.LastUpdatedBy())

	meta := UpdateMetadata{updatedAtNanos: 1000, updatedBy: "someone else"}
	err = nss.DeleteNamespace("foo", 4, meta)
	require.NoError(t, err)
	ns, err = nss.Namespace("foo")
	require.NoError(t, err)
	require.Equal(t, len(ns.snapshots), 2)
	require.Equal(t, 1, ns.Snapshots()[0].ForRuleSetVersion())
	lastSnapshot = ns.Snapshots()[1]
	require.Equal(t, 5, lastSnapshot.ForRuleSetVersion())
	require.True(t, lastSnapshot.Tombstoned())
	require.Equal(t, int64(1000), lastSnapshot.LastUpdatedAtNanos())
	require.Equal(t, "someone else", lastSnapshot.LastUpdatedBy())

	meta = UpdateMetadata{updatedAtNanos: 2000, updatedBy: "john"}
	revived, err := nss.AddNamespace("foo", meta)
	require.NoError(t, err)
	require.True(t, revived)
	ns, err = nss.Namespace("foo")
	require.NoError(t, err)
	require.False(t, ns.Tombstoned())
	require.Equal(t, len(ns.snapshots), 3)
	require.Equal(t, 1, ns.Snapshots()[0].ForRuleSetVersion())
	lastSnapshot = ns.Snapshots()[2]
	require.Equal(t, 6, lastSnapshot.ForRuleSetVersion())
	require.False(t, lastSnapshot.Tombstoned())
	require.Equal(t, int64(2000), lastSnapshot.LastUpdatedAtNanos())
	require.Equal(t, "john", lastSnapshot.LastUpdatedBy())
}
