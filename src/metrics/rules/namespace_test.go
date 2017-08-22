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
	"bytes"
	"encoding/json"
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

func TestNamespaceSchema(t *testing.T) {
	testNs := &schema.Namespace{
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
	}

	ns, err := newNameSpace(testNs)
	require.NoError(t, err)

	res, err := ns.Schema()
	require.NoError(t, err)

	require.Equal(t, testNs, res)
}

func TestNamespaceSchemaNoSnapshots(t *testing.T) {
	badNs := Namespace{
		name: []byte("foo"),
	}
	res, err := badNs.Schema()
	require.EqualError(t, err, errNilNamespaceSnapshot.Error())
	require.Nil(t, res)
}

func TestNamespaceSnapshotSchema(t *testing.T) {
	s := NamespaceSnapshot{
		forRuleSetVersion: 3,
		tombstoned:        false,
	}
	sSchema := &schema.NamespaceSnapshot{
		ForRulesetVersion: 3,
		Tombstoned:        false,
	}

	res := s.Schema()

	require.Equal(t, sSchema, res)
}

func TestNamespacesSchema(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
					&schema.NamespaceSnapshot{ForRulesetVersion: 4, Tombstoned: false},
				},
			},
			&schema.Namespace{
				Name: "foo2",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
					&schema.NamespaceSnapshot{ForRulesetVersion: 4, Tombstoned: false},
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

func TestNamespacesMarshal(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
					&schema.NamespaceSnapshot{ForRulesetVersion: 4, Tombstoned: false},
				},
			},
			&schema.Namespace{
				Name: "foo2",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
					&schema.NamespaceSnapshot{ForRulesetVersion: 4, Tombstoned: false},
				},
			},
		},
	}

	marshalledNamespaced := `{
		"version":1,
		"namespaces":[
			{"name":"foo",
			 "snapshots":[
				 {"forRuleSetVersion":123,"tombstoned":false},
				 {"forRuleSetVersion":4,"tombstoned":false}
				]},
			{"name":"foo2",
			 "snapshots":[
				 {"forRuleSetVersion":123,"tombstoned":false},
				 {"forRuleSetVersion":4,"tombstoned":false}
				]
			}
		]
	}`

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	res, err := json.Marshal(nss)
	require.NoError(t, err)

	var bb bytes.Buffer
	err = json.Compact(&bb, []byte(marshalledNamespaced))
	require.NoError(t, err)

	require.Equal(t, bb.String(), string(res))
}

func TestNamespacesUnmarshal(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
					&schema.NamespaceSnapshot{ForRulesetVersion: 4, Tombstoned: false},
				},
			},
			&schema.Namespace{
				Name: "foo2",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
					&schema.NamespaceSnapshot{ForRulesetVersion: 4, Tombstoned: false},
				},
			},
		},
	}
	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	data, err := json.Marshal(nss)
	require.NoError(t, err)
	var nss2 Namespaces
	err = json.Unmarshal(data, &nss2)
	require.NoError(t, err)

	expected, err := nss.Schema()
	require.NoError(t, err)

	actual, err := nss2.Schema()
	require.NoError(t, err)

	require.Equal(t, expected, actual)
}

func TestNamespaceTombstoned(t *testing.T) {
	ns, err := newNameSpace(&schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
			&schema.NamespaceSnapshot{ForRulesetVersion: 456, Tombstoned: true},
		},
	})

	require.NoError(t, err)
	require.True(t, ns.Tombstoned())

	ns, _ = newNameSpace(&schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{ForRulesetVersion: 123, Tombstoned: false},
			&schema.NamespaceSnapshot{ForRulesetVersion: 456, Tombstoned: false},
		},
	})

	require.False(t, ns.Tombstoned())
}

func TestNamespaceEmptySnap(t *testing.T) {
	ns, _ := newNameSpace(&schema.Namespace{
		Name:      "foo",
		Snapshots: []*schema.NamespaceSnapshot{},
	})

	require.True(t, ns.Tombstoned())
}

func TestNamespace(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
			&schema.Namespace{
				Name: "bar",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	ns, err := nss.Namespace("bar")
	require.NoError(t, err)
	require.Equal(t, string(ns.name), "bar")

	_, err = nss.Namespace("baz")
	require.Error(t, err)
}

func TestNamespaceDup(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	_, err = nss.Namespace("foo")
	require.EqualError(t, err, errMultipleNamespaceMatches.Error())
}
func TestNamespaceReviveLive(t *testing.T) {
	ns, err := newNameSpace(&schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
		},
	})
	require.NoError(t, err)
	err = ns.revive()
	require.Error(t, err)
}

func TestNamespaceMarkTombstoned(t *testing.T) {
	ns, err := newNameSpace(&schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
		},
	})
	require.NoError(t, err)
	err = ns.markTombstoned(4)
	require.NoError(t, err)

	require.True(t, ns.Tombstoned())

	require.Equal(t, ns.snapshots[len(ns.snapshots)-1].forRuleSetVersion, 4)
}

func TestNamespaceTombstoneAlreadyDead(t *testing.T) {
	ns, err := newNameSpace(&schema.Namespace{
		Name: "foo",
		Snapshots: []*schema.NamespaceSnapshot{
			&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
		},
	})
	require.NoError(t, err)
	err = ns.markTombstoned(4)
	require.Error(t, err)
}

func TestNamespaceAdd(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	nssClone, err := nss.Clone()
	require.NoError(t, err)

	err = nssClone.AddNamespace("bar")
	require.NoError(t, err)

	ns, err := nssClone.Namespace("bar")
	require.NoError(t, err)
	require.False(t, ns.Tombstoned())

	_, err = nss.Namespace("bar")
	require.Error(t, err)
}

func TestNamespaceAddDup(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	err = nss.AddNamespace("foo")
	require.Error(t, err)
}

func TestNamespaceRevive(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	_, err = nss.Namespace("foo")
	require.NoError(t, err)

	err = nss.DeleteNamespace("foo", 4)
	require.NoError(t, err)

	ns, err := nss.Namespace("foo")
	require.NoError(t, err)
	require.True(t, ns.Tombstoned())

	err = nss.AddNamespace("foo")
	require.NoError(t, err)

	ns, err = nss.Namespace("foo")
	require.NoError(t, err)
	require.Equal(t, ns.snapshots[len(ns.snapshots)-1].forRuleSetVersion, 5)
	require.Equal(t, len(ns.snapshots), 3)
}

func TestNamespaceDelete(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)
	_, err = nss.Namespace("foo")
	require.NoError(t, err)

	err = nss.DeleteNamespace("foo", 4)
	require.NoError(t, err)
	ns, err := nss.Namespace("foo")
	require.NoError(t, err)
	require.True(t, ns.Tombstoned())
	require.Equal(t, ns.snapshots[len(ns.snapshots)-1].forRuleSetVersion, 5)
}

func TestNamespaceDeleteMissing(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: false},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	err = nss.DeleteNamespace("bar", 4)
	require.Error(t, err)
}

func TestNamespaceDeleteTombstoned(t *testing.T) {
	testNss := &schema.Namespaces{
		Namespaces: []*schema.Namespace{
			&schema.Namespace{
				Name: "foo",
				Snapshots: []*schema.NamespaceSnapshot{
					&schema.NamespaceSnapshot{ForRulesetVersion: 1, Tombstoned: true},
				},
			},
		},
	}

	nss, err := NewNamespaces(1, testNss)
	require.NoError(t, err)

	err = nss.DeleteNamespace("foo", 4)
	require.Error(t, err)
}
