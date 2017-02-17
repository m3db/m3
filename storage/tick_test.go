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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3db/context"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestTickManagerTickNormalFlow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	d := time.Minute
	opts := testDatabaseOptions()
	c := context.NewCancellable()

	namespace := NewMockdatabaseNamespace(ctrl)
	namespace.EXPECT().NumSeries().Return(int64(10))
	namespace.EXPECT().Tick(d, c)
	namespaces := map[string]databaseNamespace{
		"test": namespace,
	}
	db := &mockDatabase{namespaces: namespaces, opts: opts}

	tm := newTickManager(db).(*tickManager)
	tm.c = c
	tm.sleepFn = func(time.Duration) {}

	require.NoError(t, tm.Tick(d, false))
	require.Equal(t, 0, len(tm.tokenCh))
}

func TestTickManagerTickCancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var wg sync.WaitGroup
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	d := time.Minute
	opts := testDatabaseOptions()
	c := context.NewCancellable()

	namespace := NewMockdatabaseNamespace(ctrl)
	namespace.EXPECT().NumSeries().Return(int64(10))
	namespace.EXPECT().Tick(d, c).Do(func(time.Duration, context.Cancellable) {
		ch1 <- struct{}{}
		<-ch2
	})
	namespaces := map[string]databaseNamespace{
		"test": namespace,
	}
	db := &mockDatabase{namespaces: namespaces, opts: opts}

	tm := newTickManager(db).(*tickManager)
	tm.c = c
	tm.sleepFn = func(time.Duration) {}

	wg.Add(1)
	go func() {
		defer wg.Done()

		require.Equal(t, errTickCancelled, tm.Tick(d, false))
		require.Equal(t, 0, len(tm.tokenCh))
	}()

	// Wait for tick to start
	<-ch1
	c.Cancel()
	ch2 <- struct{}{}
	wg.Wait()
}

func TestTickManagerNonForcedTickDuringOngoingTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var wg sync.WaitGroup
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	d := time.Minute
	opts := testDatabaseOptions()
	c := context.NewCancellable()

	namespace := NewMockdatabaseNamespace(ctrl)
	namespace.EXPECT().NumSeries().Return(int64(10))
	namespace.EXPECT().Tick(d, c).Do(func(time.Duration, context.Cancellable) {
		ch1 <- struct{}{}
		<-ch2
	})
	namespaces := map[string]databaseNamespace{
		"test": namespace,
	}
	db := &mockDatabase{namespaces: namespaces, opts: opts}

	tm := newTickManager(db).(*tickManager)
	tm.c = c
	tm.sleepFn = func(time.Duration) {}

	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, tm.Tick(d, false))
	}()

	// Wait for tick to start
	<-ch1
	require.Equal(t, errTickInProgress, tm.Tick(d, false))

	ch2 <- struct{}{}
	wg.Wait()

	require.Equal(t, 0, len(tm.tokenCh))
}

func TestTickManagerForcedTickDuringOngoingTick(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var wg sync.WaitGroup
	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	d := time.Minute
	opts := testDatabaseOptions()
	c := context.NewCancellable()

	namespace := NewMockdatabaseNamespace(ctrl)
	gomock.InOrder(
		namespace.EXPECT().NumSeries().Return(int64(10)),
		namespace.EXPECT().Tick(d, c).Do(func(time.Duration, context.Cancellable) {
			ch1 <- struct{}{}
			<-ch2
		}),
		namespace.EXPECT().NumSeries().Return(int64(10)),
		namespace.EXPECT().Tick(d, c),
	)
	namespaces := map[string]databaseNamespace{
		"test": namespace,
	}
	db := &mockDatabase{namespaces: namespaces, opts: opts}

	tm := newTickManager(db).(*tickManager)
	tm.c = c
	tm.sleepFn = func(time.Duration) {}

	wg.Add(3)
	go func() {
		defer wg.Done()

		require.Equal(t, errTickCancelled, tm.Tick(d, false))
	}()

	go func() {
		defer wg.Done()

		// Wait for tick to start
		<-ch1
		require.NoError(t, tm.Tick(d, true))
	}()

	go func() {
		defer wg.Done()

		for !c.IsCancelled() {
		}
		ch2 <- struct{}{}
	}()

	wg.Wait()
	require.Equal(t, 0, len(tm.tokenCh))
}
