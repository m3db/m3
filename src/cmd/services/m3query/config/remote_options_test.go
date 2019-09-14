// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"

	"github.com/m3db/m3/src/query/storage"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestMakeRemote(t *testing.T) {
	var (
		name      = "name"
		addresses = []string{"a", "b", "c"}
		global    = storage.BehaviorWarn
		overwrite = storage.BehaviorFail
	)

	remote := makeRemote(name, addresses, global, nil)
	assert.Equal(t, name, remote.Name)
	assert.Equal(t, addresses, remote.Addresses)
	assert.Equal(t, global, remote.ErrorBehavior)

	remote = makeRemote(name, addresses, global, &overwrite)
	assert.Equal(t, name, remote.Name)
	assert.Equal(t, addresses, remote.Addresses)
	assert.Equal(t, overwrite, remote.ErrorBehavior)
}

var tests = []struct {
	name              string
	cfg               string
	serveEnabled      bool
	serveAddress      string
	listenEnabled     bool
	reflectionEnabled bool
	remotes           []Remote
}{
	{
		name: "empty",
		cfg:  ``,
	},
	{
		name: "enabled, no options",
		cfg:  `enabled: true`,
	},
	{
		name: "listen disabled",
		cfg: `
enabled: false
listenAddress: "abc"
`,
	},
	{
		name: "listen enabled",
		cfg: `
enabled: true
listenAddress: "abc"
`,
		serveEnabled: true,
		serveAddress: "abc",
	}, {
		name:         "listen default",
		cfg:          `listenAddress: "abc"`,
		serveEnabled: true,
		serveAddress: "abc",
	},
	{
		name: "legacy default",
		cfg: `
enabled: true
remoteListenAddresses: ["abc","def"]
`,
		listenEnabled: true,
		remotes: []Remote{
			Remote{
				ErrorBehavior: storage.BehaviorWarn,
				Name:          "default",
				Addresses:     []string{"abc", "def"},
			},
		},
	},
	{
		name: "legacy disabled",
		cfg: `
enabled: false
remoteListenAddresses: ["abc","def"]
`,
	},
	{
		name: "legacy enabled with fail error behavior",
		cfg: `
enabled: true
remoteListenAddresses: ["abc","def"]
errorBehavior: "fail"
`,
		listenEnabled: true,
		remotes: []Remote{
			Remote{
				ErrorBehavior: storage.BehaviorFail,
				Name:          "default",
				Addresses:     []string{"abc", "def"},
			},
		},
	},
	{
		name: "multi-zone",
		cfg: `
remotes:
 - name: "foo"
   remoteListenAddresses: ["abc","def"]
`,
		listenEnabled: true,
		remotes: []Remote{
			Remote{
				ErrorBehavior: storage.BehaviorWarn,
				Name:          "foo",
				Addresses:     []string{"abc", "def"},
			},
		},
	},
	{
		name: "mixed",
		cfg: `
listenAddress: "pat rafter"
remoteListenAddresses: ["abc","def"]
errorBehavior: "fail"
reflectionEnabled: true
remotes:
 - name: "foo"
   remoteListenAddresses: ["ghi","jkl"]
   errorBehavior: "warn"
 - name: "bar"
   remoteListenAddresses: ["mno","pqr"]
`,
		listenEnabled:     true,
		serveEnabled:      true,
		reflectionEnabled: true,
		serveAddress:      "pat rafter",
		remotes: []Remote{
			Remote{
				ErrorBehavior: storage.BehaviorFail,
				Name:          "default",
				Addresses:     []string{"abc", "def"},
			},
			Remote{
				ErrorBehavior: storage.BehaviorWarn,
				Name:          "foo",
				Addresses:     []string{"ghi", "jkl"},
			},
			Remote{
				ErrorBehavior: storage.BehaviorFail,
				Name:          "bar",
				Addresses:     []string{"mno", "pqr"},
			},
		},
	},
	{
		name: "mixed disabled",
		cfg: `
enabled: false
listenAddress: "pat rafter"
remoteListenAddresses: ["abc","def"]
errorBehavior: "fail"
reflectionEnabled: false
remotes:
 - name: "foo"
   remoteListenAddresses: ["ghi","jkl"]
   errorBehavior: "warn"
 - name: "bar"
   remoteListenAddresses: ["mno","pqr"]
`,
	},
}

func TestParseRemoteOptions(t *testing.T) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *RPCConfiguration
			require.NoError(t, yaml.Unmarshal([]byte(tt.cfg), &cfg))
			rOpts := RemoteOptionsFromConfig(cfg)
			assert.Equal(t, tt.listenEnabled, rOpts.ListenEnabled())
			assert.Equal(t, tt.serveEnabled, rOpts.ServeEnabled())
			assert.Equal(t, tt.reflectionEnabled, rOpts.ReflectionEnabled())

			if tt.serveEnabled {
				assert.Equal(t, tt.serveAddress, rOpts.ServeAddress())
			}

			if tt.listenEnabled {
				assert.Equal(t, tt.remotes, rOpts.Remotes())
			}
		})
	}
}
