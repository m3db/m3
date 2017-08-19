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
	"errors"

	"github.com/m3db/m3metrics/generated/proto/schema"
)

var (
	emptyNamespaceSnapshot NamespaceSnapshot
	emptyNamespace         Namespace
	emptyNamespaces        Namespaces

	errNilNamespaceSnapshotSchema = errors.New("nil namespace snapshot schema")
	errNilNamespaceSchema         = errors.New("nil namespace schema")
	errNilNamespacesSchema        = errors.New("nil namespaces schema")
	errNilNamespaceSnapshot       = errors.New("nil namespace snapshot")
)

// NamespaceSnapshot defines a namespace snapshot for which rules are defined.
type NamespaceSnapshot struct {
	forRuleSetVersion int
	tombstoned        bool
}

func newNamespaceSnapshot(snapshot *schema.NamespaceSnapshot) (NamespaceSnapshot, error) {
	if snapshot == nil {
		return emptyNamespaceSnapshot, errNilNamespaceSnapshotSchema
	}
	return NamespaceSnapshot{
		forRuleSetVersion: int(snapshot.ForRulesetVersion),
		tombstoned:        snapshot.Tombstoned,
	}, nil
}

// ForRuleSetVersion is the ruleset version this namespace change is related to.
func (s NamespaceSnapshot) ForRuleSetVersion() int { return s.forRuleSetVersion }

// Tombstoned determines whether the namespace has been tombstoned.
func (s NamespaceSnapshot) Tombstoned() bool { return s.tombstoned }

// Schema returns the given Namespace in protobuf form
func (s NamespaceSnapshot) Schema() *schema.NamespaceSnapshot {
	return &schema.NamespaceSnapshot{
		ForRulesetVersion: int32(s.forRuleSetVersion),
		Tombstoned:        s.tombstoned,
	}
}

// Namespace stores namespace snapshots.
type Namespace struct {
	name      []byte
	snapshots []NamespaceSnapshot
}

// newNameSpace creates a new namespace.
func newNameSpace(namespace *schema.Namespace) (Namespace, error) {
	if namespace == nil {
		return emptyNamespace, errNilNamespaceSchema
	}
	snapshots := make([]NamespaceSnapshot, 0, len(namespace.Snapshots))
	for _, snapshot := range namespace.Snapshots {
		s, err := newNamespaceSnapshot(snapshot)
		if err != nil {
			return emptyNamespace, err
		}
		snapshots = append(snapshots, s)
	}
	return Namespace{
		name:      []byte(namespace.Name),
		snapshots: snapshots,
	}, nil
}

// Name is the name of the namespace.
func (n Namespace) Name() []byte { return n.name }

// Snapshots return the namespace snapshots.
func (n Namespace) Snapshots() []NamespaceSnapshot { return n.snapshots }

// Schema returns the given Namespace in protobuf form
func (n Namespace) Schema() (*schema.Namespace, error) {
	if n.snapshots == nil {
		return nil, errNilNamespaceSnapshot
	}

	res := &schema.Namespace{
		Name: string(n.name),
	}

	snapshots := make([]*schema.NamespaceSnapshot, len(n.snapshots))
	for i, s := range n.snapshots {
		snapshots[i] = s.Schema()
	}
	res.Snapshots = snapshots

	return res, nil
}

// Namespaces store the list of namespaces for which rules are defined.
type Namespaces struct {
	version    int
	namespaces []Namespace
}

// NewNamespaces creates new namespaces.
func NewNamespaces(version int, namespaces *schema.Namespaces) (Namespaces, error) {
	if namespaces == nil {
		return emptyNamespaces, errNilNamespacesSchema
	}
	nss := make([]Namespace, 0, len(namespaces.Namespaces))
	for _, namespace := range namespaces.Namespaces {
		ns, err := newNameSpace(namespace)
		if err != nil {
			return emptyNamespaces, err
		}
		nss = append(nss, ns)
	}
	return Namespaces{
		version:    version,
		namespaces: nss,
	}, nil
}

// Version returns the namespaces version.
func (nss Namespaces) Version() int { return nss.version }

// Namespaces returns the list of namespaces.
func (nss Namespaces) Namespaces() []Namespace { return nss.namespaces }

// Schema returns the given Namespaces slice in protobuf form.
func (nss Namespaces) Schema() (*schema.Namespaces, error) {
	res := &schema.Namespaces{}

	namespaces := make([]*schema.Namespace, len(nss.namespaces))
	for i, n := range nss.namespaces {
		namespace, err := n.Schema()
		if err != nil {
			return nil, err
		}
		namespaces[i] = namespace
	}
	res.Namespaces = namespaces

	return res, nil
}
