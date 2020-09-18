// +build integration

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

package integration

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage"
	xclock "github.com/m3db/m3/src/x/clock"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/atomic"
)

var (
	testBackgroundProcessExecuted atomic.Bool
	testBackgroundProcessInstance storage.BackgroundProcess = &testBackgroundProcess{}
)

type testBackgroundProcess struct {
	stopped atomic.Bool
}

func newTestBackgroundProcess(storage.Database, storage.Options) (storage.BackgroundProcess, error) {
	return testBackgroundProcessInstance, nil
}

func (p *testBackgroundProcess) Run() {
	testBackgroundProcessExecuted.Store(true)
	for !p.stopped.Load() {
		time.Sleep(time.Second)
	}
}

func (p *testBackgroundProcess) Stop() {
	p.stopped.Store(true)
	return
}

func (p *testBackgroundProcess) Report() {
	return
}

func TestRunCustomBackgroundProcess(t *testing.T) {
	testOpts := NewTestOptions(t)
	testSetup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, testOpts)
	defer testSetup.Close()

	storageOpts := testSetup.StorageOpts().
		SetBackgroundProcessFns([]storage.NewBackgroundProcessFn{newTestBackgroundProcess})
	testSetup.SetStorageOpts(storageOpts)

	log := storageOpts.InstrumentOptions().Logger()

	require.False(t, testBackgroundProcessExecuted.Load())

	// Start the server.
	require.NoError(t, testSetup.StartServer())

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
	}()

	log.Info("waiting for the custom background process to execute")
	executed := xclock.WaitUntil(func() bool {
		return testBackgroundProcessExecuted.Load()
	}, time.Minute)

	require.True(t, executed)
}
