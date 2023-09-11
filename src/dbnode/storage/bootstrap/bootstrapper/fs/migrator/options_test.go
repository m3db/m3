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

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestOptionsValidateStorageOptions(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestOptions(ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetStorageOptions(nil)
	require.Error(t, opts.Validate())

	opts = opts.SetStorageOptions(storage.DefaultTestOptions())
	require.Error(t, opts.Validate())
}

func TestOptionsValidateMigrationTaskFn(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestOptions(ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetMigrationTaskFn(nil)
	require.Error(t, opts.Validate())
}

func TestOptionsValidateInfoFilesByNamespace(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestOptions(ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetInfoFilesByNamespace(nil)
	require.Error(t, opts.Validate())
}

func TestOptionsValidateMigrationOpts(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestOptions(ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetMigrationOptions(nil)
	require.Error(t, opts.Validate())
}

func TestOptionsValidateInstrumentOpts(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestOptions(ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetInstrumentOptions(nil)
	require.Error(t, opts.Validate())
}

func TestOptionsValidateFilesystemOpts(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	opts := newTestOptions(ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetFilesystemOptions(nil)
	require.Error(t, opts.Validate())
}

func newTestOptions(ctrl *gomock.Controller) Options {
	mockOpts := storage.NewMockOptions(ctrl)
	mockOpts.EXPECT().Validate().AnyTimes()

	return NewOptions().
		SetMigrationTaskFn(func(result fs.ReadInfoFileResult) (migration.NewTaskFn, bool) {
			return nil, false
		}).
		SetInfoFilesByNamespace(make(bootstrap.InfoFilesByNamespace)).
		SetStorageOptions(mockOpts).
		SetFilesystemOptions(fs.NewOptions())
}
