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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type testBackgroundProcess struct {
	executed, reported, stopped atomic.Int32
}

func (p *testBackgroundProcess) run() {
	xclock.WaitUntil(func() bool {
		return p.reported.Load() > 0
	}, time.Minute)
	p.executed.Inc()
	xclock.WaitUntil(func() bool {
		return p.stopped.Load() > 0
	}, time.Minute)
}

func (p *testBackgroundProcess) Start() {
	go p.run()
}

func (p *testBackgroundProcess) Stop() {
	p.stopped.Inc()
	return
}

func (p *testBackgroundProcess) Report() {
	p.reported.Inc()
	return
}

func TestRunCustomBackgroundProcess(t *testing.T) {
	testOpts := NewTestOptions(t).SetReportInterval(time.Millisecond)
	testSetup := newTestSetupWithCommitLogAndFilesystemBootstrapper(t, testOpts)
	defer testSetup.Close()

	backgroundProcess := &testBackgroundProcess{}

	storageOpts := testSetup.StorageOpts().
		SetBackgroundProcessFns([]storage.NewBackgroundProcessFn{
			func(storage.Database, storage.Options) (storage.BackgroundProcess, error) {
				return backgroundProcess, nil
			}})
	testSetup.SetStorageOpts(storageOpts)

	log := storageOpts.InstrumentOptions().Logger()

	// Start the server.
	require.NoError(t, testSetup.StartServer())

	// Stop the server.
	defer func() {
		require.NoError(t, testSetup.StopServer())
		log.Debug("server is now down")
		assert.Equal(t, int32(1), backgroundProcess.stopped.Load(), "failed to stop")
	}()

	log.Info("waiting for the custom background process to execute")
	xclock.WaitUntil(func() bool {
		return backgroundProcess.executed.Load() > 0
	}, time.Minute)

	assert.Equal(t, int32(1), backgroundProcess.executed.Load(), "failed to execute")
	assert.True(t, backgroundProcess.reported.Load() > 0, "failed to report")
}
