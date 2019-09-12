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
	"github.com/m3db/m3/src/query/storage"
)

// Remote is an option for remote storage.
type Remote struct {
	// ErrorBehavior describes what this remote client should do on error.
	ErrorBehavior storage.ErrorBehavior
	// Name is the name for this remote client.
	Name string
	// Addresses are the remote addresses for this client.
	Addresses []string
}

func makeRemote(
	name string,
	addresses []string,
	global storage.ErrorBehavior,
	override *storage.ErrorBehavior,
) Remote {
	if override != nil {
		global = *override
	}

	return Remote{Name: name, Addresses: addresses, ErrorBehavior: global}
}

// RemoteOptions are the options for RPC configurations.
type RemoteOptions interface {
	// ServeEnabled describes if this RPC should serve rpc requests.
	ServeEnabled() bool
	// ServeAddress describes the address this RPC server will listening on.
	ServeAddress() string
	// ListenEnabled describes if this RPC should connect to remote clients.
	ListenEnabled() bool
	// ReflectionEnabled describes if this RPC server should have reflection
	// enabled.
	ReflectionEnabled() bool
	// Remotes is a list of remote clients.
	Remotes() []Remote
}

type remoteOptions struct {
	enabled           bool
	reflectionEnabled bool
	address           string
	remotes           []Remote
}

// RemoteOptionsFromConfig builds remote options given a set of configs.
func RemoteOptionsFromConfig(cfg *RPCConfiguration) RemoteOptions {
	if cfg == nil {
		return &remoteOptions{}
	}

	// NB: Remote storage enabled should be determined by which options are
	// present unless enabled is explicitly set to false.
	enabled := true
	if cfg.Enabled != nil {
		enabled = *cfg.Enabled
	}

	// NB: Remote storages warn on failure by default.
	defaultBehavior := storage.BehaviorWarn
	if cfg.ErrorBehavior != nil {
		defaultBehavior = *cfg.ErrorBehavior
	}

	remotes := make([]Remote, 0, len(cfg.Remotes)+1)
	if len(cfg.RemoteListenAddresses) > 0 {
		remotes = append(remotes, makeRemote("default", cfg.RemoteListenAddresses,
			defaultBehavior, nil))
	}

	for _, remote := range cfg.Remotes {
		remotes = append(remotes, makeRemote(remote.Name,
			remote.RemoteListenAddresses, defaultBehavior, remote.ErrorBehavior))
	}

	return &remoteOptions{
		enabled:           enabled,
		reflectionEnabled: cfg.ReflectionEnabled,
		address:           cfg.ListenAddress,
		remotes:           remotes,
	}
}

func (o *remoteOptions) ServeEnabled() bool {
	return o.enabled && len(o.address) > 0
}

func (o *remoteOptions) ServeAddress() string {
	return o.address
}

func (o *remoteOptions) ListenEnabled() bool {
	return o.enabled && len(o.remotes) > 0
}

func (o *remoteOptions) ReflectionEnabled() bool {
	return o.enabled && o.reflectionEnabled
}

func (o *remoteOptions) Remotes() []Remote {
	return o.remotes
}
