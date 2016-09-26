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
)

func TestFlushManagerHasFlushed(t *testing.T) {
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)

	now := time.Now()
	require.False(t, fm.HasFlushed(now))

	fm.flushStates[now] = fileOpState{Status: fileOpFailed}
	require.False(t, fm.HasFlushed(now))

	fm.flushStates[now] = fileOpState{Status: fileOpSuccess}
	require.True(t, fm.HasFlushed(now))
}

func TestFlushManagerNeedsFlush(t *testing.T) {
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)

	now := time.Now()
	maxFlushRetries := database.opts.MaxFlushRetries()
	require.True(t, fm.needsFlushWithLock(now))

	fm.flushStates[now] = fileOpState{Status: fileOpFailed, NumFailures: maxFlushRetries - 1}
	require.True(t, fm.needsFlushWithLock(now))

	fm.flushStates[now] = fileOpState{Status: fileOpFailed, NumFailures: maxFlushRetries}
	require.False(t, fm.needsFlushWithLock(now))

	fm.flushStates[now] = fileOpState{Status: fileOpSuccess}
	require.False(t, fm.needsFlushWithLock(now))
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
	fm := newFlushManager(database).(*flushManager)
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
	fm := newFlushManager(database).(*flushManager)
	for _, input := range inputs {
		require.Equal(t, input.expected, fm.FlushTimeEnd(input.ts))
	}
}

func TestFlushManagerFlush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inputTimes := []struct {
		bs time.Time
		fs fileOpState
	}{
		{time.Unix(14400, 0), fileOpState{fileOpFailed, 2}},
		{time.Unix(28800, 0), fileOpState{fileOpFailed, 3}},
		{time.Unix(36000, 0), fileOpState{fileOpInProgress, 0}},
		{time.Unix(43200, 0), fileOpState{fileOpSuccess, 1}},
	}
	notFlushed := make(map[time.Time]struct{})
	for i := 1; i < 4; i++ {
		notFlushed[inputTimes[i].bs] = struct{}{}
	}

	tickStart := time.Unix(188000, 0)
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)
	for _, input := range inputTimes {
		fm.flushStates[input.bs] = input.fs
	}
	endTime := time.Unix(0, 0).Add(2 * 24 * time.Hour)

	namespaces := make(map[string]databaseNamespace)
	for i := 0; i < 2; i++ {
		name := strconv.Itoa(i)
		ns := NewMockdatabaseNamespace(ctrl)
		ns.EXPECT().Name().Return(name)
		cur := inputTimes[0].bs
		for !cur.After(endTime) {
			if _, excluded := notFlushed[cur]; !excluded {
				ns.EXPECT().Flush(cur, fm.pm).Return(nil)
			}
			cur = cur.Add(2 * time.Hour)
		}
		ns.EXPECT().Flush(cur, fm.pm).Return(errors.New("some errors"))
		namespaces[name] = ns
	}
	database.namespaces = namespaces

	err := fm.Flush(tickStart)

	j := 0
	for i := 0; i < 19; i++ {
		if i == 1 {
			j += 3
		}
		expectedTime := time.Unix(int64(21600+j*7200), 0)
		require.Equal(t, fileOpSuccess, fm.flushStates[expectedTime].Status)
		j++
	}
	expectedTime := time.Unix(int64(180000), 0)
	require.Equal(t, fileOpFailed, fm.flushStates[expectedTime].Status)
	require.Equal(t, 1, fm.flushStates[expectedTime].NumFailures)
	require.Error(t, err)
}

func TestFlushManagerFlushTimes(t *testing.T) {
	inputTimes := []struct {
		bs time.Time
		fs fileOpState
	}{
		{time.Unix(14400, 0), fileOpState{fileOpFailed, 2}},
		{time.Unix(28800, 0), fileOpState{fileOpFailed, 3}},
		{time.Unix(36000, 0), fileOpState{fileOpInProgress, 0}},
		{time.Unix(43200, 0), fileOpState{fileOpSuccess, 1}},
	}
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)
	for _, input := range inputTimes {
		fm.flushStates[input.bs] = input.fs
	}
	tickStart := time.Unix(188000, 0)
	res := fm.flushTimes(tickStart)
	require.Equal(t, 21, len(res))
	j := 0
	for i := 0; i < len(res); i++ {
		if i == 2 {
			j += 3
		}
		expectedTime := time.Unix(int64(14400+j*7200), 0)
		require.Equal(t, expectedTime, res[20-i])
		j++
	}
}

func TestFlushManagerFlushWithTimes(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	flushTime := time.Unix(7200, 0)
	database := newMockDatabase()
	fm := newFlushManager(database).(*flushManager)

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
		ns.EXPECT().Flush(flushTime, fm.pm).Return(input.err)
		if input.err != nil {
			ns.EXPECT().Name().Return(input.name)
		}
		namespaces[input.name] = ns
	}
	database.namespaces = namespaces

	require.Error(t, fm.flushWithTime(flushTime))
}
