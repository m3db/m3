// Copyright (c) 2021 Uber Technologies, Inc.
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

package bootstrap

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/topology"
	xcontext "github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestBootstrapProcessRunActiveBlockAdvanced(t *testing.T) {
	var (
		ctrl = gomock.NewController(t)
		ctx  = xcontext.NewBackground()

		blockSize    = time.Hour
		startTime    = xtime.Now().Truncate(blockSize)
		bufferPast   = 30 * time.Minute
		bufferFuture = 30 * time.Minute
		// shift 'now' just enough so that after adding 'bufferFuture' it would reach the next block
		now = startTime.Add(blockSize - bufferFuture)

		retentionOpts = retention.NewOptions().
				SetBlockSize(blockSize).
				SetRetentionPeriod(12 * blockSize).
				SetBufferPast(bufferPast).
				SetBufferFuture(bufferFuture)
		nsOptions = namespace.NewOptions().SetRetentionOptions(retentionOpts)
		nsID      = ident.StringID("ns")
		ns, err   = namespace.NewMetadata(nsID, nsOptions)
	)
	require.NoError(t, err)

	processNs := []ProcessNamespace{
		{
			Metadata:        ns,
			Shards:          []uint32{0},
			DataAccumulator: NewMockNamespaceDataAccumulator(ctrl),
		},
	}

	bootstrapper := NewMockBootstrapper(ctrl)
	bootstrapper.EXPECT().String().Return("mock_bootstrapper").AnyTimes()
	bootstrapper.EXPECT().
		Bootstrap(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_, _, _ interface{}) (NamespaceResults, error) {
			return NewNamespaceResults(NewNamespaces(processNs)), nil
		}).
		AnyTimes()

	process := bootstrapProcess{
		processOpts:          NewProcessOptions(),
		resultOpts:           result.NewOptions(),
		fsOpts:               fs.NewOptions(),
		nowFn:                func() time.Time { return now.ToTime() },
		log:                  instrument.NewOptions().Logger(),
		bootstrapper:         bootstrapper,
		initialTopologyState: &topology.StateSnapshot{},
	}

	_, err = process.Run(ctx, startTime, processNs)
	require.Equal(t, ErrFileSetSnapshotTypeRangeAdvanced, err)
}
