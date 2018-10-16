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

package config

import (
	"github.com/m3db/m3/src/cluster/client"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3x/instrument"
)

// KVClientConfiguration configures the client for the key-value store.
// TODO(xichen): add configuration for in-memory client with pre-populated data
// for different namespaces so we can start up m3aggregator without a real etcd cluster.
type KVClientConfiguration struct {
	Etcd *etcdclient.Configuration `yaml:"etcd"`
}

// NewKVClient creates a new KV client.
func (c *KVClientConfiguration) NewKVClient(
	instrumentOpts instrument.Options,
) (client.Client, error) {
	if c.Etcd == nil {
		return nil, errNoKVClientConfiguration
	}
	return c.Etcd.NewClient(instrumentOpts)
}
