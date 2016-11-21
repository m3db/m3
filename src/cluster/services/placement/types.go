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

import (
	"errors"

	"github.com/m3db/m3cluster/services"
)

var (
	// ErrPlacementNotExist is returned when no placement was found from Storage
	ErrPlacementNotExist = errors.New("placement not exist")
)

// Algorithm places shards on instances
type Algorithm interface {
	// InitPlacement initialize a sharding placement with replica factor = 1
	InitialPlacement(instances []services.PlacementInstance, shards []uint32) (services.ServicePlacement, error)

	// AddReplica up the replica factor by 1 in the placement
	AddReplica(p services.ServicePlacement) (services.ServicePlacement, error)

	// AddInstance adds a instance to the placement
	AddInstance(p services.ServicePlacement, i services.PlacementInstance) (services.ServicePlacement, error)

	// RemoveInstance removes a instance from the placement
	RemoveInstance(p services.ServicePlacement, i services.PlacementInstance) (services.ServicePlacement, error)

	// ReplaceInstance replace a instance with new instances
	ReplaceInstance(p services.ServicePlacement, leavingInstance services.PlacementInstance, addingInstances []services.PlacementInstance) (services.ServicePlacement, error)
}

// DeploymentPlanner generates deployment steps for a placement
type DeploymentPlanner interface {
	// DeploymentSteps returns the deployment steps
	DeploymentSteps(p services.ServicePlacement) [][]services.PlacementInstance
}

// Storage provides read and write access to service placement
type Storage interface {
	// SetPlacement writes a placement for a service
	SetPlacement(service services.ServiceID, p services.ServicePlacement) error

	// Placement reads a placement for a service
	Placement(service services.ServiceID) (services.ServicePlacement, error)
}

// DeploymentOptions provides options for DeploymentPlanner
type DeploymentOptions interface {
	// MaxStepSize limits the number of instances to be deployed in one step
	MaxStepSize() int
	SetMaxStepSize(stepSize int) DeploymentOptions
}
