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

// Store performs read/write operations for rules and namespaces.
type Store interface {
	// ReadNamespaces returns the persisted namespaces in kv store.
	ReadNamespaces() (*Namespaces, error)

	// ReadRuleSet returns the persisted ruleset in kv store.
	ReadRuleSet(nsName string) (RuleSet, error)

	// WriteRuleSet saves the given ruleset to the backing store.
	WriteRuleSet(rs MutableRuleSet) error

	// WriteNamespaces saves the given namespaces to the backing store.
	WriteNamespaces(nss *Namespaces) error

	// WriteAll saves both the given ruleset and namespace to the backing store.
	WriteAll(nss *Namespaces, rs MutableRuleSet) error

	// Close closes the store.
	Close()
}
