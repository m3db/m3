// Copyright (c) 2016 Uber Technologies, Inc.
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

package placement

import "fmt"

// Algorithm places shards on hosts
type Algorithm interface {
	// InitPlacement initialize a sharding placement with RF = 1
	BuildInitialPlacement(hosts []Host, shards []uint32) (Snapshot, error)

	// AddReplica up the RF by 1 in the cluster
	AddReplica(p Snapshot) (Snapshot, error)

	// AddHost adds a host to the cluster
	AddHost(p Snapshot, h Host) (Snapshot, error)

	// RemoveHost removes a host from the cluster
	RemoveHost(p Snapshot, h Host) (Snapshot, error)

	// ReplaceHost replace a host with new hosts
	ReplaceHost(p Snapshot, leavingHost Host, addingHosts []Host) (Snapshot, error)
}

// DeploymentPlanner generates deployment steps for a placement
type DeploymentPlanner interface {
	// DeploymentSteps returns the deployment steps
	DeploymentSteps(p Snapshot) [][]HostShards
}

// Snapshot describes how shards are placed on hosts
type Snapshot interface {
	// HostShards returns all HostShards in the placement
	HostShards() []HostShards

	// HostsLen returns the length of all hosts in the placement
	HostsLen() int

	// Replicas returns the replication factor in the placement
	Replicas() int

	// ShardsLen returns the number of shards in a replica
	ShardsLen() int

	// Shards returns all the unique shard ids for a replica
	Shards() []uint32

	// HostShard returns the HostShards for the requested host
	HostShard(id string) HostShards

	// Validate checks if the snapshot is valid
	Validate() error

	// Copy copies the Snapshot
	Copy() Snapshot
}

// HostShards represents a host and its assigned shards
type HostShards interface {
	Host() Host
	Shards() []uint32
	ShardsLen() int
	AddShard(shard uint32)
	RemoveShard(shard uint32)
	ContainsShard(shard uint32) bool
}

// Host represents a weighted host
type Host interface {
	fmt.Stringer
	ID() string
	Rack() string
	Zone() string
	Weight() uint32
}

// Service handles the placement related operations for registered services
// all write or update operations will persist the generated snapshot before returning success
type Service interface {
	BuildInitialPlacement(service string, hosts []Host, shardLen int, rf int) error
	AddReplica(service string) error
	AddHost(service string, candidateHosts []Host) error
	RemoveHost(service string, host Host) error
	ReplaceHost(service string, leavingHost Host, candidateHosts []Host) error

	// Snapshot gets the persisted snapshot for service
	Snapshot(service string) (Snapshot, error)
}

// SnapshotStorage provides read and write access to placement snapshots
type SnapshotStorage interface {
	SaveSnapshotForService(service string, p Snapshot) error
	ReadSnapshotForService(service string) (Snapshot, error)
}

// Options is the interface for placement options
type Options interface {
	// LooseRackCheck enables the placement to loose the rack check
	// during host replacement to achieve full ownership transfer
	LooseRackCheck() bool
	SetLooseRackCheck(looseRackCheck bool) Options

	// AcrossZone enables the placement have hosts across zones
	AcrossZones() bool
	SetAcrossZones(acrossZones bool) Options

	// AllowPartialReplace allows shards from the leaving host to be
	// placed on hosts other than the new hosts in a replace operation
	AllowPartialReplace() bool
	SetAllowPartialReplace(allowPartialReplace bool) Options
}
