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
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

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

	namespace.EXPECT().
		NeedsFlush(now).
		Return(true)
	require.True(t, fm.NeedsFlush(now))

	namespace.EXPECT().
		NeedsFlush(now).
		Return(false)
	require.False(t, fm.NeedsFlush(now))
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
