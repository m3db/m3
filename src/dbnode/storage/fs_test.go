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

package storage

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/instrument"
)

func TestFileSystemManagerShouldRunDuringBootstrap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	database := newMockdatabase(ctrl)
	fsm := newFileSystemManager(database, nil, DefaultTestOptions())
	mgr := fsm.(*fileSystemManager)

	database.EXPECT().IsBootstrapped().Return(false)
	require.False(t, mgr.shouldRunWithLock())

	database.EXPECT().IsBootstrapped().Return(true)
	require.True(t, mgr.shouldRunWithLock())
}

func TestFileSystemManagerShouldRunWhileRunning(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	database := newMockdatabase(ctrl)
	fsm := newFileSystemManager(database, nil, DefaultTestOptions())
	mgr := fsm.(*fileSystemManager)
	database.EXPECT().IsBootstrapped().Return(true)
	require.True(t, mgr.shouldRunWithLock())
	mgr.status = fileOpInProgress
	require.False(t, mgr.shouldRunWithLock())
}

func TestFileSystemManagerShouldRunEnableDisable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	database := newMockdatabase(ctrl)
	fsm := newFileSystemManager(database, nil, DefaultTestOptions())
	mgr := fsm.(*fileSystemManager)
	database.EXPECT().IsBootstrapped().Return(true).AnyTimes()
	require.True(t, mgr.shouldRunWithLock())
	require.NotEqual(t, fileOpInProgress, mgr.Disable())
	require.False(t, mgr.shouldRunWithLock())
	mgr.Enable()
	require.True(t, mgr.shouldRunWithLock())
}

func TestFileSystemManagerRun(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	database := newMockdatabase(ctrl)
	database.EXPECT().IsBootstrapped().Return(true).AnyTimes()

	fm := NewMockdatabaseFlushManager(ctrl)
	cm := NewMockdatabaseCleanupManager(ctrl)
	fsm := newFileSystemManager(database, nil, DefaultTestOptions())
	mgr := fsm.(*fileSystemManager)
	mgr.databaseFlushManager = fm
	mgr.databaseCleanupManager = cm

	ts := time.Now()
	gomock.InOrder(
		cm.EXPECT().WarmFlushCleanup(ts).Return(errors.New("foo")),
	)

	defer instrument.SetShouldPanicEnvironmentVariable(true)()
	require.Panics(t, func() { mgr.Run(ts) })
}
