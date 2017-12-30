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
	"time"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/rules/validator/namespace"
)

// NamespaceValidatorConfiguration configures a KV-backed namespace validator.
type NamespaceValidatorConfiguration struct {
	KVConfig               kv.Configuration `yaml:"kvConfig"`
	InitWatchTimeout       *time.Duration   `yaml:"initWatchTimeout"`
	ValidNamespacesKey     string           `yaml:"validNamespacesKey" validate:"nonzero"`
	DefaultValidNamespaces *[]string        `yaml:"defaultValidNamespaces"`
}

// NewNamespaceValidator creates a new namespace validator.
func (c *NamespaceValidatorConfiguration) NewNamespaceValidator(
	kvClient client.Client,
) (namespace.Validator, error) {
	kvOpts, err := c.KVConfig.NewOptions()
	if err != nil {
		return nil, err
	}
	kvStore, err := kvClient.Store(kvOpts)
	if err != nil {
		return nil, err
	}
	opts := NewNamespaceValidatorOptions().
		SetKVStore(kvStore).
		SetValidNamespacesKey(c.ValidNamespacesKey)
	if c.InitWatchTimeout != nil {
		opts = opts.SetInitWatchTimeout(*c.InitWatchTimeout)
	}
	if c.DefaultValidNamespaces != nil {
		opts = opts.SetDefaultValidNamespaces(*c.DefaultValidNamespaces)
	}
	return NewNamespaceValidator(opts)
}
