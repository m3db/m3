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
	emptyNamespace  Namespace
	emptyNamespaces Namespaces

	errNilNamespaceSchema  = errors.New("nil namespace schema")
	errNilNamespacesSchema = errors.New("nil namespaces schema")
)

// Namespace is a logical isolation unit for which rules are defined.
type Namespace struct {
	name       []byte
	tombstoned bool
	expireAtNs int64
}

// newNameSpace creates a new namespace.
func newNameSpace(namespace *schema.Namespace) (Namespace, error) {
	if namespace == nil {
		return emptyNamespace, errNilNamespaceSchema
	}
	return Namespace{
		name:       []byte(namespace.Name),
		tombstoned: namespace.Tombstoned,
		expireAtNs: namespace.ExpireAt,
	}, nil
}

// Name is the name of the namespace.
func (n *Namespace) Name() []byte { return n.name }

// Tombstoned determines whether the namespace has been tombstoned.
func (n *Namespace) Tombstoned() bool { return n.tombstoned }

// ExpireAtNs determines when the namespace will be expired.
func (n *Namespace) ExpireAtNs() int64 { return n.expireAtNs }

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
