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

import "github.com/m3db/m3cluster/services"

// Algorithm places shards on instances
type Algorithm interface {
	// InitPlacement initialize a sharding placement with given replica factor.
	InitialPlacement(instances []services.PlacementInstance, shards []uint32, rf int) (services.Placement, error)

	// AddReplica up the replica factor by 1 in the placement.
	AddReplica(p services.Placement) (services.Placement, error)

	// AddInstances adds a list of instance to the placement.
	AddInstances(p services.Placement, instances []services.PlacementInstance) (services.Placement, error)

	// RemoveInstances removes a list of instances from the placement.
	RemoveInstances(p services.Placement, leavingInstanceIDs []string) (services.Placement, error)

	// ReplaceInstance replace a instance with new instances.
	ReplaceInstance(p services.Placement, leavingInstanceID string, addingInstances []services.PlacementInstance) (services.Placement, error)

	// IsCompatibleWith checks whether the algorithm could be applied to given placement.
	IsCompatibleWith(p services.Placement) error
}

// DeploymentPlanner generates deployment steps for a placement
type DeploymentPlanner interface {
	// DeploymentSteps returns the deployment steps
	DeploymentSteps(p services.Placement) [][]services.PlacementInstance
}

// Storage provides read and write access to service placement
type Storage interface {
	// Set writes a placement for a service
	Set(service services.ServiceID, p services.Placement) error

	// CheckAndSet writes a placement for a service if the current version
	// matches the expected version
	CheckAndSet(service services.ServiceID, p services.Placement, version int) error

	// SetIfNotExist writes a placement for a service
	SetIfNotExist(service services.ServiceID, p services.Placement) error

	// Delete deletes the placement for a service
	Delete(service services.ServiceID) error

	// Placement reads placement and version for a service
	Placement(service services.ServiceID) (services.Placement, int, error)
}

// DeploymentOptions provides options for DeploymentPlanner
type DeploymentOptions interface {
	// MaxStepSize limits the number of instances to be deployed in one step
	MaxStepSize() int
	SetMaxStepSize(stepSize int) DeploymentOptions
}
