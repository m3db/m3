//go:build big
// +build big

//
// Copyright (c) 2018 Uber Technologies, Inc.
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

package commitlog

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

// TestCommitLogActiveLogsConcurrency makes sure that the
// ActiveLogs() API behaves correctly even while concurrent
// writes are going on.
func TestCommitLogActiveLogsConcurrency(t *testing.T) {
	var (
		opts, _ = newTestOptions(t, overrides{
			strategy: StrategyWriteBehind,
		})
		numFilesRequired = 10
	)

	defer cleanup(t, opts)

	var (
		doneCh    = make(chan struct{})
		commitLog = newTestCommitLog(t, opts)
	)

	// One goroutine continuously writing.
	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
				time.Sleep(time.Millisecond)
				err := commitLog.Write(
					context.NewBackground(),
					testSeries(t, opts, 0, "foo.bar", testTags1, 127),
					ts.Datapoint{},
					xtime.Second,
					nil)
				if err == errCommitLogClosed {
					return
				}
				if err == ErrCommitLogQueueFull {
					continue
				}
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	// One goroutine continuously rotating the logs.
	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
				time.Sleep(time.Millisecond)
				_, err := commitLog.RotateLogs()
				if err == errCommitLogClosed {
					return
				}
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	// One goroutine continuously checking active logs.
	go func() {
		var (
			lastSeenFile string
			numFilesSeen int
		)
		for numFilesSeen < numFilesRequired {
			time.Sleep(100 * time.Millisecond)
			logs, err := commitLog.ActiveLogs()
			if err != nil {
				panic(err)
			}
			require.Equal(t, 2, len(logs))
			if logs[0].FilePath != lastSeenFile {
				lastSeenFile = logs[0].FilePath
				numFilesSeen++
			}
		}
		close(doneCh)
	}()

	<-doneCh

	require.NoError(t, commitLog.Close())
}

// TestCommitLogRotateLogsConcurrency makes sure that the
// RotateLogs() API behaves correctly even while concurrent
// writes are going on.
func TestCommitLogRotateLogsConcurrency(t *testing.T) {
	var (
		opts, _ = newTestOptions(t, overrides{
			strategy: StrategyWriteBehind,
		})
		numFilesRequired = 10
	)

	opts = opts.SetBlockSize(1 * time.Millisecond)
	defer cleanup(t, opts)

	var (
		doneCh    = make(chan struct{})
		commitLog = newTestCommitLog(t, opts)
	)

	// One goroutine continuously writing.
	go func() {
		for {
			select {
			case <-doneCh:
				return
			default:
				time.Sleep(time.Millisecond)
				err := commitLog.Write(
					context.NewBackground(),
					testSeries(t, opts, 0, "foo.bar", testTags1, 127),
					ts.Datapoint{},
					xtime.Second,
					nil)
				if err == errCommitLogClosed {
					return
				}
				if err == ErrCommitLogQueueFull {
					continue
				}
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	// One goroutine continuously rotating logs.
	go func() {
		var (
			lastSeenFile string
			numFilesSeen int
		)
		for numFilesSeen < numFilesRequired {
			time.Sleep(100 * time.Millisecond)
			file, err := commitLog.RotateLogs()
			if err != nil {
				panic(err)
			}
			if file.FilePath != lastSeenFile {
				lastSeenFile = file.FilePath
				numFilesSeen++
			}
		}
		close(doneCh)
	}()

	<-doneCh

	require.NoError(t, commitLog.Close())
}
