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

package config

import (
	"github.com/m3db/m3db/src/dbnode/client"
	"github.com/m3db/m3x/instrument"
)

// Configuration is the configuration for the coordinator.
type Configuration struct {
	// DBClient is the DB client configuration.
	DBClient *client.Configuration `yaml:"dbClient"`

	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// ListenAddress is the server listen address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// RPC is the RPC configuration.
	RPC *RPCConfiguration `yaml:"rpc"`
}

// RPCConfiguration is the RPC configuration for the coordinator for
// the GRPC server used for remote coordinator to coordinator calls.
type RPCConfiguration struct {
	// Enabled determines if coordinator RPC is enabled for remote calls.
	Enabled bool `yaml:"enabled"`

	// ListenAddress is the RPC server listen address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// RemoteListenAddresses is the remote listen addresses to call for remote
	// coordinator calls.
	RemoteListenAddresses []string `yaml:"remoteListenAddresses"`
}
