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

package service

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"sort"

	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
	"github.com/stretchr/testify/assert"
)

func TestGoodWorkflow(t *testing.T) {
	ms := NewMockStorage()
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), ms)

	err := ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1"), placement.NewHost("r2h2", "r2")}, 10, 1)
	assert.NoError(t, err)

	err = ps.AddReplica("serviceA")
	assert.NoError(t, err)

	err = ps.AddHost("serviceA", []placement.Host{placement.NewHost("r3h3", "r3")})
	assert.NoError(t, err)

	err = ps.RemoveHost("serviceA", placement.NewHost("r1h1", "r1"))
	assert.NoError(t, err)

	err = ps.AddHost("serviceA", []placement.Host{placement.NewHost("r1h1", "r1")})
	assert.NoError(t, err)

	err = ps.ReplaceHost("serviceA", placement.NewHost("r2h2", "r2"), []placement.Host{placement.NewHost("r2h3", "r2"), placement.NewHost("r4h4", "r4"), placement.NewHost("r5h5", "r5")})
	assert.NoError(t, err)
	s, err := ms.ReadSnapshotForService("serviceA")
	assert.NoError(t, err)
	assert.NotNil(t, s.HostShard("r2h3")) // host added from preferred rack

	err = ps.AddHost("serviceA", []placement.Host{placement.NewHost("r2h4", "r2")})
	assert.NoError(t, err)

	err = ps.AddHost("serviceA", []placement.Host{placement.NewHost("r3h4", "r3")})
	assert.NoError(t, err)
	err = ps.AddHost("serviceA", []placement.Host{placement.NewHost("r3h5", "r3")})
	assert.NoError(t, err)

	hosts := []placement.Host{
		placement.NewHost("r1h5", "r1"),
		placement.NewHost("r3h4", "r3"),
		placement.NewHost("r3h5", "r3"),
		placement.NewHost("r3h6", "r3"),
		placement.NewHost("r2h3", "r2"),
	}
	err = ps.AddHost("serviceA", hosts)
	assert.NoError(t, err)
	s, err = ms.ReadSnapshotForService("serviceA")
	assert.NoError(t, err)
	assert.NotNil(t, s.HostShard("r1h5")) // host added from most needed rack

	cleanUpTestFiles(t, "serviceA")
}

func TestBadInitialPlacement(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage())

	err := ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1"), placement.NewHost("r2h2", "r2")}, 100, 2)
	assert.NoError(t, err)

	// no shards
	err = ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1"), placement.NewHost("r2h2", "r2")}, 0, 1)
	assert.Error(t, err)

	// not enough hosts
	err = ps.BuildInitialPlacement("serviceA", []placement.Host{}, 10, 1)
	assert.Error(t, err)

	// not enough racks
	err = ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1"), placement.NewHost("r1h2", "r1")}, 100, 2)
	assert.Error(t, err)
}

func TestBadAddReplica(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage())

	err := ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1")}, 10, 1)
	assert.NoError(t, err)

	// not enough racks/hosts
	err = ps.AddReplica("serviceA")
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.AddReplica("badService")
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadAddHost(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage())

	err := ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1")}, 10, 1)
	assert.NoError(t, err)

	// adding host already exist
	err = ps.AddHost("serviceA", []placement.Host{placement.NewHost("r1h1", "r1")})
	assert.Error(t, err)

	// algo error
	psWithErrorAlgo := NewPlacementService(errorAlgorithm{}, NewMockStorage())
	err = psWithErrorAlgo.AddHost("serviceA", []placement.Host{placement.NewHost("r2h2", "r2")})
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.AddHost("badService", []placement.Host{placement.NewHost("r2h2", "r2")})
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadRemoveHost(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage())

	err := ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1")}, 10, 1)
	assert.NoError(t, err)

	// leaving host not exist
	err = ps.RemoveHost("serviceA", placement.NewHost("r2h2", "r2"))
	assert.Error(t, err)

	// not enough racks/hosts after removal
	err = ps.RemoveHost("serviceA", placement.NewHost("r1h1", "r1"))
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.RemoveHost("bad service", placement.NewHost("r1h1", "r1"))
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestBadReplaceHost(t *testing.T) {
	ps := NewPlacementService(algo.NewRackAwarePlacementAlgorithm(), NewMockStorage())

	err := ps.BuildInitialPlacement("serviceA", []placement.Host{placement.NewHost("r1h1", "r1"), placement.NewHost("r4h4", "r4")}, 10, 1)
	assert.NoError(t, err)

	// leaving host not exist
	err = ps.ReplaceHost("serviceA", placement.NewHost("r1h2", "r1"), []placement.Host{placement.NewHost("r2h2", "r2")})
	assert.Error(t, err)

	// adding host already exist
	err = ps.ReplaceHost("serviceA", placement.NewHost("r1h1", "r1"), []placement.Host{placement.NewHost("r4h4", "r4")})
	assert.Error(t, err)

	// not enough rack after replace
	err = ps.AddReplica("serviceA")
	assert.NoError(t, err)
	err = ps.ReplaceHost("serviceA", placement.NewHost("r4h4", "r4"), []placement.Host{placement.NewHost("r1h2", "r1")})
	assert.Error(t, err)

	// could not find snapshot for service
	err = ps.ReplaceHost("badService", placement.NewHost("r1h1", "r1"), []placement.Host{placement.NewHost("r2h2", "r2")})
	assert.Error(t, err)

	cleanUpTestFiles(t, "serviceA")
}

func TestRackLenSort(t *testing.T) {
	r1 := rackLen{rack: "r1", len: 1}
	r2 := rackLen{rack: "r2", len: 2}
	r3 := rackLen{rack: "r3", len: 3}
	r4 := rackLen{rack: "r4", len: 2}
	r5 := rackLen{rack: "r5", len: 1}
	r6 := rackLen{rack: "r6", len: 2}
	r7 := rackLen{rack: "r7", len: 3}
	rs := rackLens{r1, r2, r3, r4, r5, r6, r7}
	sort.Sort(rs)

	seen := 0
	for _, rl := range rs {
		assert.True(t, seen <= rl.len)
		seen = rl.len
	}
}

func cleanUpTestFiles(t *testing.T, service string) {
	err := os.Remove(getSnapshotFileName(service))
	if err != nil {
		assert.FailNow(t, err.Error())
	}
}

type errorAlgorithm struct{}

func (errorAlgorithm) BuildInitialPlacement(hosts []placement.Host, ids []uint32) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddReplica(p placement.Snapshot) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) AddHost(p placement.Snapshot, h placement.Host) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) RemoveHost(p placement.Snapshot, h placement.Host) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

func (errorAlgorithm) ReplaceHost(p placement.Snapshot, leavingHost, addingHost placement.Host) (placement.Snapshot, error) {
	return nil, errors.New("error in errorAlgorithm")
}

// file based snapshot storage
type mockStorage struct{}

const configFileSuffix = "_placement.json"

func getSnapshotFileName(service string) string {
	return service + configFileSuffix
}

func NewMockStorage() placement.SnapshotStorage {
	return mockStorage{}
}

func (ms mockStorage) SaveSnapshotForService(service string, p placement.Snapshot) error {
	var err error
	if err = p.Validate(); err != nil {
		return err
	}
	var data []byte
	if data, err = json.Marshal(p); err != nil {
		return err
	}
	return ioutil.WriteFile(getSnapshotFileName(service), data, 0644)
}

func (ms mockStorage) ReadSnapshotForService(service string) (placement.Snapshot, error) {
	var data []byte
	var err error
	if data, err = ioutil.ReadFile(getSnapshotFileName(service)); err != nil {
		return nil, err
	}
	return placement.NewPlacementFromJSON(data)
}
