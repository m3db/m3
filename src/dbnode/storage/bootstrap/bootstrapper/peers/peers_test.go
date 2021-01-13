// Copyright (c) 2017 Uber Technologies, Inc.
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

package peers

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/runtime"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/index/compaction"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPeersBootstrapperInvalidOpts(t *testing.T) {
	_, err := NewPeersBootstrapperProvider(NewOptions(), nil)
	assert.Error(t, err)
}

func TestNewPeersBootstrapper(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	idxOpts := index.NewOptions()
	compactor, err := compaction.NewCompactor(idxOpts.MetadataArrayPool(),
		index.MetadataArrayPoolCapacity,
		idxOpts.SegmentBuilderOptions(),
		idxOpts.FSTSegmentOptions(),
		compaction.CompactorOptions{
			FSTWriterOptions: &fst.WriterOptions{
				// DisableRegistry is set to true to trade a larger FST size
				// for a faster FST compaction since we want to reduce the end
				// to end latency for time to first index a metric.
				DisableRegistry: true,
			},
		})
	require.NoError(t, err)

	fsOpts := fs.NewOptions()
	icm, err := fs.NewIndexClaimsManager(fsOpts)
	require.NoError(t, err)
	opts := NewOptions().
		SetFilesystemOptions(fs.NewOptions()).
		SetIndexOptions(idxOpts).
		SetAdminClient(client.NewMockAdminClient(ctrl)).
		SetPersistManager(persist.NewMockManager(ctrl)).
		SetIndexClaimsManager(icm).
		SetFilesystemOptions(fsOpts).
		SetCompactor(compactor).
		SetRuntimeOptionsManager(runtime.NewMockOptionsManager(ctrl))

	b, err := NewPeersBootstrapperProvider(opts, nil)
	require.NoError(t, err)
	assert.Equal(t, PeersBootstrapperName, b.String())
}
