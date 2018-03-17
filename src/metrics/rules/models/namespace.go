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

// Namespace is a common json serializable namespace.
type Namespace struct {
	ID                string `json:"id" validate:"required"`
	ForRuleSetVersion int    `json:"forRuleSetVersion"`
}

// NamespaceView is a human friendly representation of a namespace at a single point in time.
type NamespaceView struct {
	Name               string
	ForRuleSetVersion  int
	Tombstoned         bool
	LastUpdatedAtNanos int64
	LastUpdatedBy      string
}

// NewNamespace takes a NamespaceView returns the equivalent Namespace.
func NewNamespace(nv *NamespaceView) Namespace {
	return Namespace{
		ID:                nv.Name,
		ForRuleSetVersion: nv.ForRuleSetVersion,
	}
}

// Namespaces is a common json serializable list of namespaces.
type Namespaces struct {
	Version    int         `json:"version"`
	Namespaces []Namespace `json:"namespaces"`
}

// NamespacesView is a representation of all the namespaces at a point in time.
type NamespacesView struct {
	Version    int
	Namespaces []*NamespaceView
}

// NewNamespaces takes a NamespacesView returns the equivalent Namespaces.
func NewNamespaces(nss *NamespacesView) Namespaces {
	views := make([]Namespace, len(nss.Namespaces))
	for i, namespace := range nss.Namespaces {
		views[i] = NewNamespace(namespace)
	}
	return Namespaces{
		Version:    nss.Version,
		Namespaces: views,
	}
}
