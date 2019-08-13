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

package namespace

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func newTestNamespaceWatch(t *testing.T, ctrl *gomock.Controller, updater NamespaceUpdater) (
	NamespaceWatch,
	*MockWatch,
) {
	testScope := tally.NewTestScope("", nil)
	iopts := instrument.NewOptions().
		SetMetricsScope(testScope).
		SetReportInterval(10 * time.Millisecond)

	mockWatch := NewMockWatch(ctrl)

	return NewNamespaceWatch(updater, mockWatch, iopts), mockWatch
}

func TestNamespaceWatchStartStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	ch := make(chan struct{}, 1)
	defer func() {
		ctrl.Finish()
		close(ch)
		leaktest.CheckTimeout(t, time.Second)()
	}()

	noopUpdater := func(Map) error {return nil}
	dbWatch, mockWatch := newTestNamespaceWatch(t, ctrl, noopUpdater)

	mockWatch.EXPECT().C().Return(ch).AnyTimes()
	// start and stop
	require.NoError(t, dbWatch.Start())
	require.NoError(t, dbWatch.Stop())

	// start and close
	require.NoError(t, dbWatch.Start())
	require.NoError(t, dbWatch.Close())
}

func TestNamespaceWatchStopWithoutStart(t *testing.T) {
	ctrl := gomock.NewController(t)
	ch := make(chan struct{}, 1)
	defer func() {
		ctrl.Finish()
		close(ch)
		leaktest.CheckTimeout(t, time.Second)()
	}()

	noopUpdater := func(Map) error {return nil}
	dbWatch, _ := newTestNamespaceWatch(t, ctrl, noopUpdater)
	require.Error(t, dbWatch.Stop())
	require.NoError(t, dbWatch.Close())
}

func TestNamespaceWatchUpdatePropagation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ch := make(chan struct{}, 1)
	defer func() {
		ctrl.Finish()
		close(ch)
		leaktest.CheckTimeout(t, time.Second)()
	}()

	mockMap := NewMockMap(ctrl)
	mockMap.EXPECT().Metadatas().Return([]Metadata{}).AnyTimes()

	noopUpdater := func(Map) error {return nil}
	dbWatch, mockWatch := newTestNamespaceWatch(t, ctrl, noopUpdater)
	mockWatch.EXPECT().Get().Return(mockMap).AnyTimes()
	mockWatch.EXPECT().C().Return(ch).AnyTimes()
	require.NoError(t, dbWatch.Start())

	ch <- struct{}{}
	time.Sleep(100 * time.Millisecond) // give test a chance to schedule pending go-routines
	require.NoError(t, dbWatch.Stop())
}
