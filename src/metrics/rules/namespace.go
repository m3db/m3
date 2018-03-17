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
	"fmt"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/rules/models"
	xerrors "github.com/m3db/m3x/errors"
)

var (
	emptyNamespaceSnapshot NamespaceSnapshot
	emptyNamespace         Namespace
	emptyNamespaces        Namespaces

	errNamespaceSnapshotIndexOutOfRange = errors.New("namespace snapshot idx out of range")
	errNilNamespaceSnapshotSchema       = errors.New("nil namespace snapshot schema")
	errNilNamespaceSchema               = errors.New("nil namespace schema")
	errNilNamespacesSchema              = errors.New("nil namespaces schema")
	errNilNamespaceSnapshot             = errors.New("nil namespace snapshot")
	errMultipleNamespaceMatches         = errors.New("more than one namespace match found")
	errNamespaceNotFound                = errors.New("namespace not found")
	errNamespaceNotTombstoned           = errors.New("namespace is not tombstoned")
	errNamespaceAlreadyTombstoned       = errors.New("namespace is already tombstoned")
	errNoNamespaceSnapshots             = errors.New("namespace has no snapshots")

	namespaceActionErrorFmt = "cannot %s namespace %s"
)

// NamespaceSnapshot defines a namespace snapshot for which rules are defined.
type NamespaceSnapshot struct {
	forRuleSetVersion  int
	tombstoned         bool
	lastUpdatedAtNanos int64
	lastUpdatedBy      string
}

func newNamespaceSnapshot(snapshot *schema.NamespaceSnapshot) (NamespaceSnapshot, error) {
	if snapshot == nil {
		return emptyNamespaceSnapshot, errNilNamespaceSnapshotSchema
	}
	return NamespaceSnapshot{
		forRuleSetVersion:  int(snapshot.ForRulesetVersion),
		tombstoned:         snapshot.Tombstoned,
		lastUpdatedAtNanos: snapshot.LastUpdatedAtNanos,
		lastUpdatedBy:      snapshot.LastUpdatedBy,
	}, nil
}

// ForRuleSetVersion is the ruleset version this namespace change is related to.
func (s NamespaceSnapshot) ForRuleSetVersion() int { return s.forRuleSetVersion }

// Tombstoned determines whether the namespace has been tombstoned.
func (s NamespaceSnapshot) Tombstoned() bool { return s.tombstoned }

// LastUpdatedAtNanos returns the time when the namespace is last updated in nanoseconds.
func (s NamespaceSnapshot) LastUpdatedAtNanos() int64 { return s.lastUpdatedAtNanos }

// LastUpdatedBy returns the user who last updated the namespace.
func (s NamespaceSnapshot) LastUpdatedBy() string { return s.lastUpdatedBy }

// Schema returns the given Namespace in protobuf form
func (s NamespaceSnapshot) Schema() *schema.NamespaceSnapshot {
	return &schema.NamespaceSnapshot{
		ForRulesetVersion:  int32(s.forRuleSetVersion),
		Tombstoned:         s.tombstoned,
		LastUpdatedAtNanos: s.lastUpdatedAtNanos,
		LastUpdatedBy:      s.lastUpdatedBy,
	}
}

// Namespace stores namespace snapshots.
type Namespace struct {
	name      []byte
	snapshots []NamespaceSnapshot
}

// newNamespace creates a new namespace.
func newNamespace(namespace *schema.Namespace) (Namespace, error) {
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

// NamespaceView returns the view representation of a namespace object.
func (n Namespace) NamespaceView(snapshotIdx int) (*models.NamespaceView, error) {
	if snapshotIdx < 0 || snapshotIdx >= len(n.snapshots) {
		return nil, errNamespaceSnapshotIndexOutOfRange
	}
	s := n.snapshots[snapshotIdx]
	return &models.NamespaceView{
		Name:               string(n.name),
		ForRuleSetVersion:  s.forRuleSetVersion,
		Tombstoned:         s.tombstoned,
		LastUpdatedAtNanos: s.lastUpdatedAtNanos,
		LastUpdatedBy:      s.lastUpdatedBy,
	}, nil
}

func (n Namespace) clone() Namespace {
	name := make([]byte, len(n.name))
	copy(name, n.name)
	snapshots := make([]NamespaceSnapshot, len(n.snapshots))
	copy(snapshots, n.snapshots)
	return Namespace{
		name:      name,
		snapshots: snapshots,
	}
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

func (n *Namespace) markTombstoned(tombstonedRSVersion int, meta UpdateMetadata) error {
	if n.Tombstoned() {
		return errNamespaceAlreadyTombstoned
	}
	snapshot := NamespaceSnapshot{
		forRuleSetVersion:  tombstonedRSVersion,
		tombstoned:         true,
		lastUpdatedAtNanos: meta.updatedAtNanos,
		lastUpdatedBy:      meta.updatedBy,
	}
	n.snapshots = append(n.snapshots, snapshot)
	return nil
}

func (n *Namespace) revive(meta UpdateMetadata) error {
	if !n.Tombstoned() {
		return errNamespaceNotTombstoned
	}
	if len(n.snapshots) == 0 {
		return errNoNamespaceSnapshots
	}

	tombstonedRuleSetVersion := n.snapshots[len(n.snapshots)-1].forRuleSetVersion
	// NB: The revived ruleset version is one after the tombstoned ruleset version.
	snapshot := NamespaceSnapshot{
		forRuleSetVersion:  tombstonedRuleSetVersion + 1,
		tombstoned:         false,
		lastUpdatedAtNanos: meta.updatedAtNanos,
		lastUpdatedBy:      meta.updatedBy,
	}
	n.snapshots = append(n.snapshots, snapshot)
	return nil
}

// Tombstoned returns the tombstoned state for a given namespace.
func (n Namespace) Tombstoned() bool {
	if len(n.snapshots) == 0 {
		return true
	}
	return n.snapshots[len(n.snapshots)-1].tombstoned
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
		ns, err := newNamespace(namespace)
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

// NamespacesView returns a view representation of a given Namespaces object.
func (nss Namespaces) NamespacesView() (*models.NamespacesView, error) {
	namespaces := make([]*models.NamespaceView, len(nss.namespaces))
	for i, n := range nss.namespaces {
		ns, err := n.NamespaceView(len(n.snapshots) - 1)
		if err != nil {
			return nil, err
		}
		namespaces[i] = ns
	}
	return &models.NamespacesView{
		Version:    nss.version,
		Namespaces: namespaces,
	}, nil
}

// Clone creates a deep copy of this Namespaces object.
func (nss Namespaces) Clone() Namespaces {
	namespaces := make([]Namespace, len(nss.namespaces))
	for i, n := range nss.namespaces {
		namespaces[i] = n.clone()
	}
	return Namespaces{
		version:    nss.version,
		namespaces: namespaces,
	}
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

// Namespace returns a namespace with a given name.
func (nss *Namespaces) Namespace(name string) (*Namespace, error) {
	var res *Namespace

	for i, ns := range nss.namespaces {
		if string(ns.name) != name {
			continue
		}

		if res == nil {
			res = &nss.namespaces[i]
		} else {
			return nil, errMultipleNamespaceMatches
		}
	}

	if res == nil {
		return nil, errNamespaceNotFound
	}

	return res, nil
}

// AddNamespace adds a new namespace to the namespaces structure and persists it.
// This function returns a boolean indicating whether or not the namespace was revived.
// The revived flag should be used to decided if the corresponding" ruleset should also
// be revived.
func (nss *Namespaces) AddNamespace(nsName string, meta UpdateMetadata) (bool, error) {
	existing, err := nss.Namespace(nsName)
	if err != nil && err != errNamespaceNotFound {
		return false, xerrors.Wrap(err, fmt.Sprintf(namespaceActionErrorFmt, "add", nsName))
	}

	// Brand new namespace.
	if err == errNamespaceNotFound {
		ns := Namespace{
			name: []byte(nsName),
			snapshots: []NamespaceSnapshot{
				NamespaceSnapshot{
					forRuleSetVersion:  1,
					tombstoned:         false,
					lastUpdatedAtNanos: meta.updatedAtNanos,
					lastUpdatedBy:      meta.updatedBy,
				},
			},
		}

		nss.namespaces = append(nss.namespaces, ns)
		return false, nil
	}

	// Revive the namespace.
	if err = existing.revive(meta); err != nil {
		return false, xerrors.Wrap(err, fmt.Sprintf(namespaceActionErrorFmt, "revive", nsName))
	}

	return true, nil
}

// DeleteNamespace tombstones the given namespace mapping it to the next ruleset version.
func (nss *Namespaces) DeleteNamespace(nsName string, currRuleSetVersion int, meta UpdateMetadata) error {
	existing, err := nss.Namespace(nsName)
	if err != nil {
		return xerrors.Wrap(err, fmt.Sprintf(namespaceActionErrorFmt, "delete", nsName))
	}

	if err := existing.markTombstoned(currRuleSetVersion+1, meta); err != nil {
		return xerrors.Wrap(err, fmt.Sprintf(namespaceActionErrorFmt, "delete", nsName))
	}

	return nil
}
