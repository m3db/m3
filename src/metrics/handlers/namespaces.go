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
	"fmt"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/generated/proto/schema"
)

// Namespaces returns the version and the persisted namespaces in kv store.
func Namespaces(store kv.Store, namespacesKey string) (int, *schema.Namespaces, error) {
	value, err := store.Get(namespacesKey)
	if err != nil {
		return 0, nil, err
	}

	version := value.Version()
	var namespaces schema.Namespaces
	if err := value.Unmarshal(&namespaces); err != nil {
		return 0, nil, err
	}

	return version, &namespaces, nil
}

// ValidateNamespace validates whether a given namespace exists.
func ValidateNamespace(store kv.Store, namespacesKey string, namespaceName string) (int, *schema.Namespaces, *schema.Namespace, error) {
	namespacesVersion, namespaces, err := Namespaces(store, namespacesKey)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("could not read namespaces data: %v", err)
	}
	ns, err := Namespace(namespaces, namespaceName)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("error finding namespace with name %s: %v", namespaceName, err)
	}
	if ns == nil {
		return 0, nil, nil, fmt.Errorf("namespace %s doesn't exist", namespaceName)
	}
	if len(ns.Snapshots) == 0 {
		return 0, nil, nil, fmt.Errorf("namespace %s has no snapshots", namespaceName)
	}
	if ns.Snapshots[len(ns.Snapshots)-1].Tombstoned {
		return 0, nil, nil, fmt.Errorf("namespace %s is tombstoned", namespaceName)
	}
	return namespacesVersion, namespaces, ns, nil
}

// Namespace returns the namespace with a given name, or an error if there are
// multiple matches.
func Namespace(namespaces *schema.Namespaces, namespaceName string) (*schema.Namespace, error) {
	var namespace *schema.Namespace
	for _, ns := range namespaces.Namespaces {
		if ns.Name != namespaceName {
			continue
		}
		if namespace == nil {
			namespace = ns
		} else {
			return nil, errMultipleMatches
		}
	}

	if namespace == nil {
		return nil, kv.ErrNotFound
	}

	return namespace, nil
}
