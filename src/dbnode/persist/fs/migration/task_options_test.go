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

package migration

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/ident"
	"github.com/stretchr/testify/require"
)

func defaultTestOptions(t *testing.T, ctrl *gomock.Controller) TaskOptions {
	md, err := namespace.NewMetadata(ident.StringID("ns"), namespace.NewOptions())
	require.NoError(t, err)

	pm, err := fs.NewPersistManager(fs.NewOptions())
	require.NoError(t, err)

	mockOpts := storage.NewMockOptions(ctrl)
	mockOpts.EXPECT().Validate().AnyTimes()

	return NewTaskOptions().
		SetInfoFileResult(fs.ReadInfoFileResult{}).
		SetShard(1).
		SetNamespaceMetadata(md).
		SetPersistManager(pm).
		SetStorageOptions(mockOpts).
		SetFilesystemOptions(fs.NewOptions())
}

func TestValidateStorageOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := defaultTestOptions(t, ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetStorageOptions(nil)
	require.Error(t, opts.Validate())
}

func TestValidateNewMergerFn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := defaultTestOptions(t, ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetNewMergerFn(nil)
	require.Error(t, opts.Validate())
}

func TestValidateInfoResultErr(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := defaultTestOptions(t, ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetInfoFileResult(fs.ReadInfoFileResult{Err: testReadInfoFileResultError{}})
	require.Error(t, opts.Validate())
}

func TestValidateNamespaceMetadata(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := defaultTestOptions(t, ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetNamespaceMetadata(nil)
	require.Error(t, opts.Validate())
}

func TestValidatePersistManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := defaultTestOptions(t, ctrl)
	require.NoError(t, opts.Validate())

	opts = opts.SetPersistManager(nil)
	require.Error(t, opts.Validate())
}

func TestInfoFileResult(t *testing.T) {
	opts := NewTaskOptions()
	value := fs.ReadInfoFileResult{}
	require.Equal(t, value, opts.SetInfoFileResult(value).InfoFileResult())
}

func TestShard(t *testing.T) {
	opts := NewTaskOptions()
	value := uint32(1)
	require.Equal(t, value, opts.SetShard(value).Shard())
}

func TestNamespaceMetadata(t *testing.T) {
	opts := NewTaskOptions()
	value, err := namespace.NewMetadata(ident.StringID("ns"), namespace.NewOptions())
	require.NoError(t, err)

	require.Equal(t, value, opts.SetNamespaceMetadata(value).NamespaceMetadata())
}

func TestPersistManager(t *testing.T) {
	opts := NewTaskOptions()
	value, err := fs.NewPersistManager(fs.NewOptions())
	require.NoError(t, err)

	require.Equal(t, value, opts.SetPersistManager(value).PersistManager())
}

func TestStorageOptions(t *testing.T) {
	opts := NewTaskOptions()
	value := storage.DefaultTestOptions()
	require.Equal(t, value, opts.SetStorageOptions(value).StorageOptions())
}

type testReadInfoFileResultError struct {
}

func (t testReadInfoFileResultError) Error() error {
	return fmt.Errorf("error")
}

func (t testReadInfoFileResultError) Filepath() string {
	return ""
}
