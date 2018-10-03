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
	"time"

	"github.com/m3db/m3/src/query/storage/m3"
	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3x/config/listenaddress"
	"github.com/m3db/m3x/instrument"
)

// BackendStorageType is an enum for different backends
type BackendStorageType string

const (
	// GRPCStorageType is for backends which only support grpc endpoints
	GRPCStorageType BackendStorageType = "grpc"
	// M3DBStorageType is for m3db backend
	M3DBStorageType BackendStorageType = "m3db"
)

// Configuration is the configuration for the query service.
type Configuration struct {
	// Metrics configuration.
	Metrics instrument.MetricsConfiguration `yaml:"metrics"`

	// Clusters is the DB cluster configurations for read, write and
	// query endpoints.
	Clusters m3.ClustersStaticConfiguration `yaml:"clusters"`

	// LocalConfiguration is the local embedded configuration if running
	// coordinator embedded in the DB.
	Local *LocalConfiguration `yaml:"local"`

	// ClusterManagement for placemement, namespaces and database management
	// endpoints (optional).
	ClusterManagement *ClusterManagementConfiguration `yaml:"clusterManagement"`

	// ListenAddress is the server listen address.
	ListenAddress *listenaddress.Configuration `yaml:"listenAddress" validate:"nonzero"`

	// RPC is the RPC configuration.
	RPC *RPCConfiguration `yaml:"rpc"`

	// Backend is the backend store for query service. We currently support grpc and m3db (default).
	Backend BackendStorageType `yaml:"backend"`

	// ReadWorkerPoolSize is the size of the worker pool for read requests.
	ReadWorkerPoolSize int `yaml:"readWorkerPoolSize"`

	// WriteWorkerPoolSize is the size of the worker pool for write requests.
	WriteWorkerPoolSize int `yaml:"writeWorkerPoolSize"`
}

// LocalConfiguration is the local embedded configuration if running
// coordinator embedded in the DB.
type LocalConfiguration struct {
	// Namespace is the name of the local namespace to write/read from.
	Namespace string `yaml:"namespace" validate:"nonzero"`

	// Retention is the retention of the local namespace to write/read from.
	Retention time.Duration `yaml:"retention" validate:"nonzero"`
}

// ClusterManagementConfiguration is configuration for the placemement,
// namespaces and database management endpoints (optional).
type ClusterManagementConfiguration struct {
	// Etcd is the client configuration for etcd.
	Etcd etcdclient.Configuration `yaml:"etcd"`
}

// RPCConfiguration is the RPC configuration for the coordinator for
// the GRPC server used for remote coordinator to coordinator calls.
type RPCConfiguration struct {
	// Enabled determines if coordinator RPC is enabled for remote calls.
	Enabled bool `yaml:"enabled"`

	// ListenAddress is the RPC server listen address.
	ListenAddress string `yaml:"listenAddress"`

	// RemoteListenAddresses is the remote listen addresses to call for remote
	// coordinator calls.
	RemoteListenAddresses []string `yaml:"remoteListenAddresses"`
}
