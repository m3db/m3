/*
 * Copyright (c) 2020 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package migrator

import (
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/stretchr/testify/require"
)

func TestMigratorRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testMigratorOptions(t, ctrl)

	md1, err := namespace.NewMetadata(ident.StringID("foo"), namespace.NewOptions())
	require.NoError(t, err)

	md2, err := namespace.NewMetadata(ident.StringID("bar"), namespace.NewOptions())
	require.NoError(t, err)

	// Create some dummy ReadInfoFileResults as these are used to determine if we need to run a migration or not
	infoFilesByNamespace := map[namespace.Metadata]fs.ShardsInfoFilesResult{
		md1: {
			1: {fs.ReadInfoFileResult{}, fs.ReadInfoFileResult{}},
			2: {fs.ReadInfoFileResult{}, fs.ReadInfoFileResult{}},
		},
		md2: {
			1: {fs.ReadInfoFileResult{}, fs.ReadInfoFileResult{}},
			2: {fs.ReadInfoFileResult{}, fs.ReadInfoFileResult{}},
		},
	}
	var runs int32
	opts = opts.
		SetNewMigrationFn(func(opts migration.TaskOptions) (migration.Task, error) {
			return &testTask{runs: &runs}, nil
		}).
		SetShouldMigrateFn(func(_ fs.ReadInfoFileResult) bool {
			return true
		}).
		SetInfoFilesByNamespace(infoFilesByNamespace).
		SetMigrationOptions(migration.NewOptions().SetConcurrency(4)) // Set concurrency to half tasks to queue up work

	migrator, err := NewMigrator(opts)
	require.NoError(t, err)

	err = migrator.Run()
	require.NoError(t, err)

	require.Equal(t, int32(8), runs)
}

func TestMigratorRun_ShouldNotMigrate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testMigratorOptions(t, ctrl)

	md1, err := namespace.NewMetadata(ident.StringID("foo"), namespace.NewOptions())
	require.NoError(t, err)

	// Create some dummy ReadInfoFileResults as these are used to determine if we need to run a migration or not
	infoFilesByNamespace := map[namespace.Metadata]fs.ShardsInfoFilesResult{
		md1: {
			1: {fs.ReadInfoFileResult{}, fs.ReadInfoFileResult{}},
		},
	}
	var runs int32
	opts = opts.
		SetNewMigrationFn(func(opts migration.TaskOptions) (migration.Task, error) {
			return &testTask{runs: &runs}, nil
		}).
		SetShouldMigrateFn(func(_ fs.ReadInfoFileResult) bool {
			// Nothing should be considered in a migratable state
			return false
		}).
		SetInfoFilesByNamespace(infoFilesByNamespace).
		SetMigrationOptions(migration.NewOptions().SetConcurrency(4))

	migrator, err := NewMigrator(opts)
	require.NoError(t, err)

	err = migrator.Run()
	require.NoError(t, err)

	require.Equal(t, int32(0), runs)
}

func testMigratorOptions(t *testing.T, ctrl *gomock.Controller) Options {
	mockOpts := storage.NewMockOptions(ctrl)
	mockOpts.EXPECT().Validate().AnyTimes()

	return NewOptions().
		SetInstrumentOptions(instrument.NewOptions()).
		SetMigrationOptions(migration.NewOptions()).
		SetFilesystemOptions(fs.NewOptions()).
		SetStorageOptions(mockOpts)
}

type testTask struct {
	runs *int32
}

func (t *testTask) Run() error {
	atomic.AddInt32(t.runs, 1)
	return nil
}
