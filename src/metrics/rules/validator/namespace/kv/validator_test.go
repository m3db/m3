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

package kv

import (
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

const (
	testValidNamespacesKey = "validNamespaces"
)

var (
	testNamespaces1 = []string{"foo", "bar", "baz"}
	testNamespaces2 = []string{"blah", "invalid"}
)

func TestValidatorInitTimeoutWithUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	store := mem.NewStore()
	v := testValidator(t, store)

	// Timed out getting initial value and by default
	// no namespace is valid.
	for _, ns := range testNamespaces1 {
		require.Error(t, v.Validate(ns))
	}
	for _, ns := range testNamespaces2 {
		require.Error(t, v.Validate(ns))
	}

	// Set valid namespaces in KV and wait for changes to be processed.
	nss := &commonpb.StringArrayProto{Values: testNamespaces1}
	_, err := store.Set(testValidNamespacesKey, nss)
	require.NoError(t, err)
	for {
		v.RLock()
		currValidNamespaces := v.validNamespaces
		v.RUnlock()
		if len(currValidNamespaces) == len(testNamespaces1) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Now validating the valid namespaces should be ok and validating
	// invalid namespaces should return an error.
	for _, ns := range testNamespaces1 {
		require.NoError(t, v.Validate(ns))
	}
	for _, ns := range testNamespaces2 {
		require.Error(t, v.Validate(ns))
	}

	// Close the validator.
	require.False(t, v.closed)
	v.Close()
	require.True(t, v.closed)

	// Validating after the validator is closed results in an error.
	for _, ns := range testNamespaces1 {
		require.Equal(t, errValidatorClosed, v.Validate(ns))
	}
	for _, ns := range testNamespaces2 {
		require.Equal(t, errValidatorClosed, v.Validate(ns))
	}
}

func TestValidatorWithUpdateAndDeletion(t *testing.T) {
	defer leaktest.Check(t)()

	// Set valid namespaces in KV and wait for changes to be processed.
	store := mem.NewStore()
	nss := &commonpb.StringArrayProto{Values: testNamespaces1}
	_, err := store.Set(testValidNamespacesKey, nss)
	require.NoError(t, err)

	// Assert that the validator has successfully received the list
	// of valid namespaces from KV.
	v := testValidator(t, store)
	for _, ns := range testNamespaces1 {
		require.NoError(t, v.Validate(ns))
	}
	for _, ns := range testNamespaces2 {
		require.Error(t, v.Validate(ns))
	}

	// Change valid namespaces in KV and wait for changes to be processed.
	nss = &commonpb.StringArrayProto{Values: testNamespaces2}
	_, err = store.Set(testValidNamespacesKey, nss)
	require.NoError(t, err)
	for {
		v.RLock()
		currValidNamespaces := v.validNamespaces
		v.RUnlock()
		if len(currValidNamespaces) == len(testNamespaces2) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Now validating the valid namespaces should be ok and validating
	// invalid namespaces should return an error.
	for _, ns := range testNamespaces1 {
		require.Error(t, v.Validate(ns))
	}
	for _, ns := range testNamespaces2 {
		require.NoError(t, v.Validate(ns))
	}

	// Delete the valid namespaces key.
	_, err = store.Delete(testValidNamespacesKey)
	require.NoError(t, err)
	for {
		v.RLock()
		currValidNamespaces := v.validNamespaces
		v.RUnlock()
		if len(currValidNamespaces) == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// No namespaces are valid by default after the key is deleted.
	for _, ns := range testNamespaces1 {
		require.Error(t, v.Validate(ns))
	}
	for _, ns := range testNamespaces2 {
		require.Error(t, v.Validate(ns))
	}

	// Close the validator.
	require.False(t, v.closed)
	v.Close()
	require.True(t, v.closed)

	// Validating after the validator is closed results in an error.
	for _, ns := range testNamespaces1 {
		require.Equal(t, errValidatorClosed, v.Validate(ns))
	}
	for _, ns := range testNamespaces2 {
		require.Equal(t, errValidatorClosed, v.Validate(ns))
	}
}

func testValidatorOptions(store kv.Store) NamespaceValidatorOptions {
	return NewNamespaceValidatorOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetKVStore(store).
		SetValidNamespacesKey(testValidNamespacesKey)
}

func testValidator(t *testing.T, store kv.Store) *validator {
	v, err := NewNamespaceValidator(testValidatorOptions(store))
	require.NoError(t, err)
	return v.(*validator)
}
