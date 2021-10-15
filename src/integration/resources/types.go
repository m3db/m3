// Copyright (c) 2021  Uber Technologies, Inc.
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

// Package resources contains integration test resources for spinning up M3
// components.
package resources

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/x/errors"
)

// ResponseVerifier is a function that checks if the query response is valid.
type ResponseVerifier func(int, map[string][]string, string, error) error

// GoalStateVerifier verifies that the given results are valid.
type GoalStateVerifier func(string, error) error

// Coordinator is a wrapper for a coordinator. It provides a wrapper on HTTP
// endpoints that expose cluster management APIs as well as read and write
// endpoints for series data.
// TODO: consider having this work on underlying structures.
type Coordinator interface {
	Admin

	// ApplyKVUpdate applies a KV update.
	ApplyKVUpdate(update string) error
	// WriteCarbon writes a carbon metric datapoint at a given time.
	WriteCarbon(port int, metric string, v float64, t time.Time) error
	// WriteProm writes a prometheus metric.
	WriteProm(name string, tags map[string]string, samples []prompb.Sample) error
	// RunQuery runs the given query with a given verification function.
	RunQuery(verifier ResponseVerifier, query string, headers map[string][]string) error
}

// Admin is a wrapper for admin functions.
type Admin interface {
	// GetNamespace gets namespaces.
	GetNamespace() (admin.NamespaceGetResponse, error)
	// WaitForNamespace blocks until the given namespace is enabled.
	// NB: if the name string is empty, this will instead
	// check for a successful response.
	WaitForNamespace(name string) error
	// AddNamespace adds a namespace.
	AddNamespace(admin.NamespaceAddRequest) (admin.NamespaceGetResponse, error)
	// UpdateNamespace updates the namespace.
	UpdateNamespace(admin.NamespaceUpdateRequest) (admin.NamespaceGetResponse, error)
	// DeleteNamespace removes the namespace.
	DeleteNamespace(namespaceID string) error
	// CreateDatabase creates a database.
	CreateDatabase(admin.DatabaseCreateRequest) (admin.DatabaseCreateResponse, error)
	// GetPlacement gets placements.
	GetPlacement(PlacementRequestOptions) (admin.PlacementGetResponse, error)
	// InitPlacement initializes placements.
	InitPlacement(PlacementRequestOptions, admin.PlacementInitRequest) (admin.PlacementGetResponse, error)
	// WaitForInstances blocks until the given instance is available.
	WaitForInstances(ids []string) error
	// WaitForShardsReady waits until all shards gets ready.
	WaitForShardsReady() error
	// InitM3msgTopic initializes an m3msg topic.
	InitM3msgTopic(M3msgTopicOptions, admin.TopicInitRequest) (admin.TopicGetResponse, error)
	// GetM3msgTopic gets an m3msg topic.
	GetM3msgTopic(M3msgTopicOptions) (admin.TopicGetResponse, error)
	// AddM3msgTopicConsumer adds a consumer service to an m3msg topic.
	AddM3msgTopicConsumer(M3msgTopicOptions, admin.TopicAddRequest) (admin.TopicGetResponse, error)
	// WaitForClusterReady waits until the cluster is ready to receive reads and writes.
	WaitForClusterReady() error
	// Close closes the wrapper and releases any held resources, including
	// deleting docker containers.
	Close() error
}

// Node is a wrapper for a db node. It provides a wrapper on HTTP
// endpoints that expose cluster management APIs as well as read and write
// endpoints for series data.
// TODO: consider having this work on underlying structures.
type Node interface {
	// HostDetails returns this node's host details on the given port.
	HostDetails(port int) (*admin.Host, error)
	// Health gives this node's health.
	Health() (*rpc.NodeHealthResult_, error)
	// WaitForBootstrap blocks until the node has bootstrapped.
	WaitForBootstrap() error
	// WritePoint writes a datapoint to the node directly.
	WritePoint(req *rpc.WriteRequest) error
	// WriteTaggedPoint writes a datapoint with tags to the node directly.
	WriteTaggedPoint(req *rpc.WriteTaggedRequest) error
	// WriteTaggedBatchRaw writes a batch of writes to the node directly.
	WriteTaggedBatchRaw(req *rpc.WriteTaggedBatchRawRequest) error
	// AggregateTiles starts tiles aggregation, waits until it will complete
	// and returns the amount of aggregated tiles.
	AggregateTiles(req *rpc.AggregateTilesRequest) (int64, error)
	// Fetch fetches datapoints.
	Fetch(req *rpc.FetchRequest) (*rpc.FetchResult_, error)
	// FetchTagged fetches datapoints by tag.
	FetchTagged(req *rpc.FetchTaggedRequest) (*rpc.FetchTaggedResult_, error)
	// Exec executes the given commands on the node container, returning
	// stdout and stderr from the container.
	Exec(commands ...string) (string, error)
	// GoalStateExec executes the given commands on the node container, retrying
	// until applying the verifier returns no error or the default timeout.
	GoalStateExec(verifier GoalStateVerifier, commands ...string) error
	// Restart restarts this container.
	Restart() error
	// Close closes the wrapper and releases any held resources, including
	// deleting docker containers.
	Close() error
}

// Aggregator is an aggregator instance.
type Aggregator interface {
	// IsHealthy determines whether an instance is healthy.
	IsHealthy() error
	// Status returns the instance status.
	Status() (aggregator.RuntimeStatus, error)
	// Resign asks an aggregator instance to give up its current leader role if applicable.
	Resign() error
	// Close closes the wrapper and releases any held resources, including
	// deleting docker containers.
	Close() error
}

// M3Resources represents a set of test M3 components.
type M3Resources interface {
	// Cleanup cleans up after each started component.
	Cleanup() error
	// Nodes returns all node resources.
	Nodes() Nodes
	// Coordinator returns the coordinator resource.
	Coordinator() Coordinator
}

// ExternalResources represents an external (i.e. non-M3)
// resource that we'd like to be able to spin up for an
// integration test.
type ExternalResources interface {
	// Setup sets up the external resource so that it's ready
	// for use.
	Setup() error

	// Close stops and cleans up all the resources associated with
	// the external resource.
	Close() error
}

// ClusterOptions represents a set of options for a cluster setup.
type ClusterOptions struct {
	ReplicationFactor  int32
	NumShards          int32
	NumIsolationGroups int32
}

// Nodes is a slice of nodes.
type Nodes []Node

// WaitForHealthy waits for each Node in Nodes to be healthy
// and bootstrapped before returning.
func (n Nodes) WaitForHealthy() error {
	var (
		multiErr errors.MultiError
		mu       sync.Mutex
		wg       sync.WaitGroup
	)

	for _, node := range n {
		wg.Add(1)
		node := node
		go func() {
			defer wg.Done()
			err := node.WaitForBootstrap()
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return multiErr.FinalError()
}

// M3msgTopicOptions represents a set of options for an m3msg topic.
type M3msgTopicOptions struct {
	// Zone is the zone of the m3msg topic.
	Zone string
	// Env is the environment of the m3msg topic.
	Env string
	// TopicName is the topic name of the m3msg topic name.
	TopicName string
}

// PlacementRequestOptions represents a set of options for placement-related requests.
type PlacementRequestOptions struct {
	// Service is the type of service for the placement request.
	Service ServiceType
	// Env is the environment of the placement.
	Env string
	// Zone is the zone of the placement.
	Zone string
}

// ServiceType represents the type of an m3 service.
type ServiceType int

const (
	// ServiceTypeUnknown is an unknown service type.
	ServiceTypeUnknown ServiceType = iota
	// ServiceTypeM3DB represents M3DB service.
	ServiceTypeM3DB
	// ServiceTypeM3Aggregator represents M3aggregator service.
	ServiceTypeM3Aggregator
	// ServiceTypeM3Coordinator represents M3coordinator service.
	ServiceTypeM3Coordinator
)
