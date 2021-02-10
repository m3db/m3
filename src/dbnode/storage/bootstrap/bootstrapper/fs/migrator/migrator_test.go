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

package migrator

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMigratorRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testMigratorOptions(ctrl)

	md1, err := namespace.NewMetadata(ident.StringID("foo"), namespace.NewOptions())
	require.NoError(t, err)

	md2, err := namespace.NewMetadata(ident.StringID("bar"), namespace.NewOptions())
	require.NoError(t, err)

	// Create some dummy ReadInfoFileResults as these are used to determine if we need to run a migration or not.
	// Put some in a state requiring migrations and others not to flex both paths.
	infoFilesByNamespace := bootstrap.InfoFilesByNamespace{
		md1: {
			1: {testInfoFileWithVolumeIndex(0), testInfoFileWithVolumeIndex(1)},
			2: {testInfoFileWithVolumeIndex(0), testInfoFileWithVolumeIndex(1)},
		},
		md2: {
			1: {testInfoFileWithVolumeIndex(0), testInfoFileWithVolumeIndex(0)},
			2: {testInfoFileWithVolumeIndex(0), testInfoFileWithVolumeIndex(1)},
		},
	}

	opts = opts.
		SetMigrationTaskFn(func(result fs.ReadInfoFileResult) (migration.NewTaskFn, bool) {
			return newTestTask, result.Info.VolumeIndex == 0
		}).
		SetInfoFilesByNamespace(infoFilesByNamespace).
		SetMigrationOptions(migration.NewOptions())

	migrator, err := NewMigrator(opts)
	require.NoError(t, err)

	err = migrator.Run(context.NewBackground())
	require.NoError(t, err)

	// Ensure every info file has a volume index of one.
	for _, resultsByShard := range infoFilesByNamespace {
		for _, results := range resultsByShard {
			for _, info := range results {
				require.Equal(t, 1, info.Info.VolumeIndex)
			}
		}
	}
}

func testMigratorOptions(ctrl *gomock.Controller) Options {
	mockOpts := storage.NewMockOptions(ctrl)
	mockOpts.EXPECT().Validate().AnyTimes()

	return NewOptions().
		SetInstrumentOptions(instrument.NewOptions()).
		SetMigrationOptions(migration.NewOptions()).
		SetFilesystemOptions(fs.NewOptions()).
		SetStorageOptions(mockOpts)
}

type testTask struct {
	opts migration.TaskOptions
}

func newTestTask(opts migration.TaskOptions) (migration.Task, error) {
	return &testTask{opts: opts}, nil
}

func (t *testTask) Run() (fs.ReadInfoFileResult, error) {
	result := t.opts.InfoFileResult()
	result.Info.VolumeIndex += 1
	return result, nil
}

func testInfoFileWithVolumeIndex(volumeIndex int) fs.ReadInfoFileResult {
	return fs.ReadInfoFileResult{Info: schema.IndexInfo{VolumeIndex: volumeIndex}}
}
