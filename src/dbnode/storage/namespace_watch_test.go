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

package storage

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/instrument"
	xtest "github.com/m3db/m3x/test"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func newTestNamespaceWatch(t *testing.T, ctrl *gomock.Controller) (
	databaseNamespaceWatch,
	*Mockdatabase,
	*namespace.MockWatch,
) {
	testScope := tally.NewTestScope("", nil)
	iopts := instrument.NewOptions().
		SetMetricsScope(testScope).
		SetReportInterval(10 * time.Millisecond)

	// NB: Can't use golang.Mock generated database here because we need to use
	// EXPECT on unexported methods, which fails due to: https://github.com/golang/mock/issues/52
	mockDB := NewMockdatabase(ctrl)
	mockWatch := namespace.NewMockWatch(ctrl)

	return newDatabaseNamespaceWatch(mockDB, mockWatch, iopts), mockDB, mockWatch
}

func TestNamespaceWatchStartStop(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	ch := make(chan struct{}, 1)
	defer func() {
		ctrl.Finish()
		close(ch)
		leaktest.CheckTimeout(t, time.Second)()
	}()

	dbWatch, _, mockWatch := newTestNamespaceWatch(t, ctrl)

	mockWatch.EXPECT().C().Return(ch).AnyTimes()
	// start and stop
	require.NoError(t, dbWatch.Start())
	require.NoError(t, dbWatch.Stop())

	// start and close
	require.NoError(t, dbWatch.Start())
	require.NoError(t, dbWatch.Close())
}

func TestNamespaceWatchStopWithoutStart(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	ch := make(chan struct{}, 1)
	defer func() {
		ctrl.Finish()
		close(ch)
		leaktest.CheckTimeout(t, time.Second)()
	}()

	dbWatch, _, _ := newTestNamespaceWatch(t, ctrl)
	require.Error(t, dbWatch.Stop())
	require.NoError(t, dbWatch.Close())
}

func TestNamespaceWatchUpdatePropagation(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	ch := make(chan struct{}, 1)
	defer func() {
		ctrl.Finish()
		close(ch)
		leaktest.CheckTimeout(t, time.Second)()
	}()

	mockMap := namespace.NewMockMap(ctrl)
	mockMap.EXPECT().Metadatas().Return([]namespace.Metadata{}).AnyTimes()
	dbWatch, mockDB, mockWatch := newTestNamespaceWatch(t, ctrl)
	mockWatch.EXPECT().Get().Return(mockMap).AnyTimes()
	mockWatch.EXPECT().C().Return(ch).AnyTimes()
	require.NoError(t, dbWatch.Start())

	mockDB.EXPECT().UpdateOwnedNamespaces(mockMap).Return(nil)
	ch <- struct{}{}
	time.Sleep(100 * time.Millisecond) // give test a chance to schedule pending go-routines
	require.NoError(t, dbWatch.Stop())
}
