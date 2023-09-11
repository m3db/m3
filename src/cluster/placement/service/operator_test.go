// Copyright (c) 2020 Uber Technologies, Inc.
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
//

package service

import (
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperator(t *testing.T) {
	type testDeps struct {
		options placement.Options
		op      placement.Operator
	}
	setup := func(t *testing.T) testDeps {
		options := placement.NewOptions().SetAllowAllZones(true)
		return testDeps{
			options: options,
			op:      NewPlacementOperator(nil, WithPlacementOptions(options)),
		}
	}

	t.Run("errors when operations called on unset placement", func(t *testing.T) {
		tdeps := setup(t)
		_, _, err := tdeps.op.AddInstances([]placement.Instance{newTestInstance()})
		require.Error(t, err)
	})

	t.Run("BuildInitialPlacement twice errors", func(t *testing.T) {
		tdeps := setup(t)
		_, err := tdeps.op.BuildInitialPlacement([]placement.Instance{newTestInstance()}, 10, 1)
		require.NoError(t, err)

		_, err = tdeps.op.BuildInitialPlacement([]placement.Instance{newTestInstance()}, 10, 1)
		assertErrContains(t, err, "placement already exists and can't be rebuilt")
	})

	t.Run("end-to-end flow", func(t *testing.T) {
		tdeps := setup(t)
		op := NewPlacementOperator(nil, WithPlacementOptions(tdeps.options))
		store := newMockStorage()

		pl, err := op.BuildInitialPlacement([]placement.Instance{newTestInstance()}, 10, 1)
		require.NoError(t, err)

		initialVersion := pl.Version()

		_, _, err = op.AddInstances([]placement.Instance{newTestInstance()})
		require.NoError(t, err)

		_, err = op.MarkAllShardsAvailable()
		require.NoError(t, err)

		_, err = op.BalanceShards()
		require.NoError(t, err)

		_, err = store.SetIfNotExist(op.Placement())
		require.NoError(t, err)

		pl, err = store.Placement()
		require.NoError(t, err)

		// expect exactly one version increment, from store.SetIfNotExist
		assert.Equal(t, initialVersion+1, pl.Version())

		// spot check the results
		allAvailable := true
		instances := pl.Instances()
		assert.Len(t, instances, 2)
		for _, inst := range instances {
			allAvailable = allAvailable && inst.IsAvailable()
		}
		assert.True(t, allAvailable)
	})
}

type dummyStoreTestDeps struct {
	store *dummyStore
	pl    placement.Placement
}

func dummyStoreSetup(t *testing.T) dummyStoreTestDeps {
	return dummyStoreTestDeps{
		store: newDummyStore(nil),
		pl:    placement.NewPlacement(),
	}
}

func TestDummyStore_Set(t *testing.T) {
	t.Run("sets without touching version", func(t *testing.T) {
		tdeps := dummyStoreSetup(t)
		testSetsCorrectly(t, tdeps, tdeps.store.Set)
	})
}

func TestDummyStore_Placement(t *testing.T) {
	t.Run("returns placement", func(t *testing.T) {
		tdeps := dummyStoreSetup(t)

		store := newDummyStore(tdeps.pl)
		actual, err := store.Placement()
		require.NoError(t, err)
		assert.Equal(t, actual, tdeps.pl)
	})

	t.Run("errors when nil", func(t *testing.T) {
		tdeps := dummyStoreSetup(t)
		_, err := tdeps.store.Placement()
		assertErrContains(t, err, "no initial placement specified at operator construction")
	})
}

func TestDummyStore_CheckAndSet(t *testing.T) {
	t.Run("sets without touching version", func(t *testing.T) {
		tdeps := dummyStoreSetup(t)
		testSetsCorrectly(t, tdeps, func(pl placement.Placement) (placement.Placement, error) {
			return tdeps.store.CheckAndSet(pl, 5)
		})
	})

	t.Run("ignores version mismatches", func(t *testing.T) {
		tdeps := dummyStoreSetup(t)
		_, err := tdeps.store.CheckAndSet(tdeps.pl, 2)
		require.NoError(t, err)

		_, err = tdeps.store.CheckAndSet(tdeps.pl.SetVersion(5), 3)
		require.NoError(t, err)

		pl, err := tdeps.store.Placement()
		require.NoError(t, err)
		assert.Equal(t, tdeps.pl, pl)
	})
}

func TestDummyStore_SetIfNotExists(t *testing.T) {
	t.Run("sets without touching version", func(t *testing.T) {
		tdeps := dummyStoreSetup(t)
		testSetsCorrectly(t, tdeps, func(pl placement.Placement) (placement.Placement, error) {
			return tdeps.store.SetIfNotExist(pl)
		})
	})

	t.Run("errors if placement exists", func(t *testing.T) {
		tdeps := dummyStoreSetup(t)
		_, err := tdeps.store.SetIfNotExist(tdeps.pl)
		require.NoError(t, err)
		_, err = tdeps.store.SetIfNotExist(tdeps.pl)
		assertErrContains(t, err, "placement already exists and can't be rebuilt")
	})
}

// Run a *Set* function and check that it returned the right thing, didn't touch the version,
// and actually set the value.
func testSetsCorrectly(t *testing.T, tdeps dummyStoreTestDeps, set func(pl placement.Placement) (placement.Placement, error)) {
	_, err := tdeps.store.Placement()
	// should not be set yet
	require.Error(t, err)

	curVersion := tdeps.pl.Version()
	rtn, err := set(tdeps.pl)
	require.NoError(t, err)

	assert.Equal(t, curVersion, rtn.Version())
	assert.Equal(t, tdeps.pl, rtn)
	curState, err := tdeps.store.Placement()
	require.NoError(t, err)
	assert.Equal(t, tdeps.pl, curState)
}

func assertErrContains(t *testing.T, err error, contained string) {
	require.Error(t, err)
	assert.Contains(t, err.Error(), contained)
}
