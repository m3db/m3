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
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func testCleanupManager(ctrl *gomock.Controller) (*mockDatabase, *MockdatabaseFlushManager, *cleanupManager) {
	db := newMockDatabase()
	fm := NewMockdatabaseFlushManager(ctrl)
	return db, fm, newCleanupManager(db, fm, tally.NoopScope).(*cleanupManager)
}

func TestCleanupManagerCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ts := time.Unix(36000, 0)
	start := time.Unix(14400, 0)
	end := time.Unix(28800, 0)
	db, fm, mgr := testCleanupManager(ctrl)

	inputs := []struct {
		name string
		err  error
	}{
		{"foo", errors.New("some error")},
		{"bar", errors.New("some other error")},
		{"baz", nil},
	}
	namespaces := make(map[string]databaseNamespace)
	for _, input := range inputs {
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().CleanupFileset(start).Return(input.err)
		namespaces[input.name] = ns
	}
	db.namespaces = namespaces

	mgr.commitLogFilesBeforeFn = func(_ string, t time.Time) ([]string, error) {
		return []string{"foo", "bar"}, errors.New("error1")
	}
	mgr.commitLogFilesForTimeFn = func(_ string, t time.Time) ([]string, error) {
		if t == time.Unix(14400, 0) {
			return []string{"baz"}, nil
		}
		return nil, errors.New("error" + strconv.Itoa(int(t.Unix())))
	}
	var deletedFiles []string
	mgr.deleteFilesFn = func(files []string) error {
		deletedFiles = append(deletedFiles, files...)
		return nil
	}

	gomock.InOrder(
		fm.EXPECT().FlushTimeStart(ts).Return(start),
		fm.EXPECT().FlushTimeStart(ts).Return(start),
		fm.EXPECT().FlushTimeEnd(ts).Return(end),
		fm.EXPECT().NeedsFlush(time.Unix(14400, 0)).Return(false),
		fm.EXPECT().NeedsFlush(time.Unix(21600, 0)).Return(false),
		fm.EXPECT().NeedsFlush(time.Unix(28800, 0)).Return(false),
		fm.EXPECT().NeedsFlush(time.Unix(7200, 0)).Return(false),
		fm.EXPECT().NeedsFlush(time.Unix(14400, 0)).Return(false),
		fm.EXPECT().NeedsFlush(time.Unix(21600, 0)).Return(false),
		fm.EXPECT().NeedsFlush(time.Unix(0, 0)).Return(true),
	)

	require.Error(t, mgr.Cleanup(ts))
	require.Equal(t, []string{"foo", "bar", "baz"}, deletedFiles)
}

func TestCleanupManagerCommitLogTimeRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, fm, mgr := testCleanupManager(ctrl)
	ts := time.Unix(21600, 0)
	start := time.Unix(7200, 0)
	end := time.Unix(14400, 0)
	fm.EXPECT().FlushTimeStart(ts).Return(start)
	fm.EXPECT().FlushTimeEnd(ts).Return(end)
	cs, ce := mgr.commitLogTimeRange(ts)
	require.Equal(t, time.Unix(0, 0), cs)
	require.Equal(t, time.Unix(7200, 0), ce)
}
