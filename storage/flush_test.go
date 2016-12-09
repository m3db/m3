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
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3db/ts"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestFlushManagerHasFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	namespace := NewMockdatabaseNamespace(ctrl)

	db := newMockDatabase()
	db.namespaces = map[string]databaseNamespace{
		"testns": namespace,
	}

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	now := time.Now()

	namespace.EXPECT().
		FlushState(now).
		Return(fileOpState{Status: fileOpNotStarted})
	require.False(t, fm.HasFlushed(now))

	namespace.EXPECT().
		FlushState(now).
		Return(fileOpState{Status: fileOpFailed})
	require.False(t, fm.HasFlushed(now))

	namespace.EXPECT().
		FlushState(now).
		Return(fileOpState{Status: fileOpSuccess})
	require.True(t, fm.HasFlushed(now))
}

func TestFlushManagerNeedsFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	namespace := NewMockdatabaseNamespace(ctrl)

	db := newMockDatabase()
	db.namespaces = map[string]databaseNamespace{
		"testns": namespace,
	}

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	now := time.Now()

	maxRetries := db.opts.MaxFlushRetries()
	require.True(t, maxRetries > 1)

	namespace.EXPECT().
		FlushState(now).
		Return(fileOpState{Status: fileOpNotStarted})
	assert.True(t, fm.needsFlush(now))

	namespace.EXPECT().
		FlushState(now).
		Return(fileOpState{Status: fileOpFailed, NumFailures: maxRetries - 1})
	assert.True(t, fm.needsFlush(now))

	namespace.EXPECT().
		FlushState(now).
		Return(fileOpState{Status: fileOpFailed, NumFailures: maxRetries})
	assert.False(t, fm.needsFlush(now))

	namespace.EXPECT().
		FlushState(now).
		Return(fileOpState{Status: fileOpSuccess})
	require.False(t, fm.needsFlush(now))
}

func TestFlushManagerFlushTimeStart(t *testing.T) {
	inputs := []struct {
		ts       time.Time
		expected time.Time
	}{
		{time.Unix(86400*2, 0), time.Unix(0, 0)},
		{time.Unix(86400*2+7200, 0), time.Unix(7200, 0)},
		{time.Unix(86400*2+10800, 0), time.Unix(7200, 0)},
	}
	database := newMockDatabase()
	fm := newFlushManager(database, tally.NoopScope).(*flushManager)
	for _, input := range inputs {
		require.Equal(t, input.expected, fm.FlushTimeStart(input.ts))
	}
}

func TestFlushManagerFlushTimeEnd(t *testing.T) {
	inputs := []struct {
		ts       time.Time
		expected time.Time
	}{
		{time.Unix(7800, 0), time.Unix(0, 0)},
		{time.Unix(8000, 0), time.Unix(0, 0)},
		{time.Unix(15200, 0), time.Unix(7200, 0)},
	}
	database := newMockDatabase()
	fm := newFlushManager(database, tally.NoopScope).(*flushManager)
	for _, input := range inputs {
		require.Equal(t, input.expected, fm.FlushTimeEnd(input.ts))
	}
}

func TestFlushManagerFlushStates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inputTimes := []struct {
		bs time.Time
		fs fileOpState
	}{
		{time.Unix(14400, 0), fileOpState{fileOpFailed, 2}},
		{time.Unix(21600, 0), fileOpState{fileOpFailed, 3}},
		{time.Unix(28800, 0), fileOpState{fileOpFailed, 3}},
		{time.Unix(36000, 0), fileOpState{fileOpInProgress, 0}},
		{time.Unix(43200, 0), fileOpState{fileOpSuccess, 1}},
	}

	tickStart := time.Unix(51300, 0)

	db := newMockDatabase()
	db.opts = db.opts.SetRetentionOptions(
		db.opts.RetentionOptions().
			SetRetentionPeriod(10 * time.Hour))

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	namespaces := make(map[string]databaseNamespace)
	for i := 0; i < 2; i++ {
		name := "testns" + strconv.Itoa(i)
		ns := NewMockdatabaseNamespace(ctrl)
		for _, input := range inputTimes {
			ns.EXPECT().FlushState(input.bs).Return(input.fs).AnyTimes()
		}
		firstBlockStart := inputTimes[0].bs
		ns.EXPECT().Flush(firstBlockStart, fm.pm).Return(errors.New("some errors"))
		ns.EXPECT().ID().Return(ts.StringID(name))
		namespaces[name] = ns
	}
	db.namespaces = namespaces

	err := fm.Flush(tickStart)
	require.Error(t, err)

	var strs []string
	for i := 0; i < len(namespaces); i++ {
		msgFmt := "namespace testns%d failed to flush data: some errors"
		strs = append(strs, fmt.Sprintf(msgFmt, i))
	}
	assert.True(t, strings.Join(strs, "\n") == err.Error() ||
		strings.Join(reversableStrings(strs), "\n") == err.Error())
}

func TestFlushManagerFlushTimes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inputTimes := []struct {
		bs time.Time
		fs fileOpState
	}{
		{time.Unix(14400, 0), fileOpState{fileOpFailed, 2}},
		{time.Unix(21600, 0), fileOpState{fileOpFailed, 2}},
		{time.Unix(28800, 0), fileOpState{fileOpFailed, 3}},
		{time.Unix(36000, 0), fileOpState{fileOpInProgress, 0}},
		{time.Unix(43200, 0), fileOpState{fileOpSuccess, 1}},
	}

	tickStart := time.Unix(51300, 0)

	db := newMockDatabase()
	db.opts = db.opts.SetRetentionOptions(
		db.opts.RetentionOptions().
			SetRetentionPeriod(10 * time.Hour))

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	namespaces := make(map[string]databaseNamespace)
	for i := 0; i < 2; i++ {
		name := "testns" + strconv.Itoa(i)
		ns := NewMockdatabaseNamespace(ctrl)
		for _, input := range inputTimes {
			ns.EXPECT().FlushState(input.bs).Return(input.fs).AnyTimes()
		}
		namespaces[name] = ns
	}
	db.namespaces = namespaces

	res := fm.flushTimes(tickStart)

	var timestamps []int
	for _, t := range res {
		timestamps = append(timestamps, int(t.Unix()))
	}

	sort.Ints(timestamps)

	assert.Equal(t, []int{14400, 21600}, timestamps)
}

func TestFlushManagerFlushAlreadyInProgress(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inputTimes := []struct {
		bs time.Time
		fs fileOpState
	}{
		{time.Unix(14400, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(21600, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(28800, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(36000, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(43200, 0), fileOpState{fileOpNotStarted, 0}},
	}

	tickStart := time.Unix(51300, 0)

	db := newMockDatabase()
	db.opts = db.opts.SetRetentionOptions(
		db.opts.RetentionOptions().
			SetRetentionPeriod(10 * time.Hour))

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	var (
		namespaces = make(map[string]databaseNamespace)
		flushingWg sync.WaitGroup
		doneWg     sync.WaitGroup
	)
	flushingWg.Add(1)
	doneWg.Add(1)
	for i := 0; i < 2; i++ {
		name := "testns" + strconv.Itoa(i)
		ns := NewMockdatabaseNamespace(ctrl)
		for _, input := range inputTimes {
			ns.EXPECT().FlushState(input.bs).Return(input.fs).AnyTimes()
		}
		last := inputTimes[len(inputTimes)-1].bs
		if i == 0 {
			ns.EXPECT().
				Flush(last, fm.pm).
				Do(func(x interface{}, y interface{}) {
					flushingWg.Done()
					doneWg.Wait()
				}).
				Return(nil)
		} else {
			ns.EXPECT().
				Flush(last, fm.pm).
				Return(nil)
		}
		namespaces[name] = ns
	}
	db.namespaces = namespaces

	go func() {
		// Wait for flush
		flushingWg.Wait()

		err := fm.Flush(tickStart)
		assert.Error(t, err)
		assert.Equal(t, errFlushAlreadyInProgress, err)

		// Notify done
		doneWg.Done()
	}()

	err := fm.Flush(tickStart)
	assert.NoError(t, err)
}

func TestFlushManagerFlushNoTimes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inputTimes := []struct {
		bs time.Time
		fs fileOpState
	}{
		{time.Unix(14400, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(21600, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(28800, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(36000, 0), fileOpState{fileOpSuccess, 0}},
		{time.Unix(43200, 0), fileOpState{fileOpSuccess, 0}},
	}

	tickStart := time.Unix(51300, 0)

	db := newMockDatabase()
	db.opts = db.opts.SetRetentionOptions(
		db.opts.RetentionOptions().
			SetRetentionPeriod(10 * time.Hour))

	fm := newFlushManager(db, tally.NoopScope).(*flushManager)

	var (
		namespaces = make(map[string]databaseNamespace)
		flushingWg sync.WaitGroup
		doneWg     sync.WaitGroup
	)
	flushingWg.Add(1)
	doneWg.Add(1)
	for i := 0; i < 2; i++ {
		name := "testns" + strconv.Itoa(i)
		ns := NewMockdatabaseNamespace(ctrl)
		for _, input := range inputTimes {
			ns.EXPECT().FlushState(input.bs).Return(input.fs).AnyTimes()
		}
		namespaces[name] = ns
	}
	db.namespaces = namespaces

	assert.NoError(t, fm.Flush(tickStart))
}

type reversableStrings []string

func (v reversableStrings) Len() int {
	return len(v)
}

func (v reversableStrings) Less(lhs, rhs int) bool {
	return v[lhs] > v[rhs]
}

func (v reversableStrings) Swap(lhs, rhs int) {
	v[lhs], v[rhs] = v[rhs], v[lhs]
}
