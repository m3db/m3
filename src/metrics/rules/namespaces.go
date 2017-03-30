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
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
)

// Namespaces capture the list of namespaces for which rules are defined
type Namespaces struct {
	namespaces     []string
	ruleSetCutover time.Time
	version        int
}

// NewNamespaces creates new namespaces
func NewNamespaces(nss *schema.Namespaces) Namespaces {
	return Namespaces{
		namespaces:     nss.Namespaces,
		ruleSetCutover: time.Unix(0, nss.RulesetCutoverTime),
		version:        int(nss.Version),
	}
}

// Namespaces returns the list of namespaces
func (nss Namespaces) Namespaces() []string { return nss.namespaces }

// RuleSetCutover returns the ruleset cutover time
func (nss Namespaces) RuleSetCutover() time.Time { return nss.ruleSetCutover }

// Version returns the namespaces version
func (nss Namespaces) Version() int { return nss.version }
