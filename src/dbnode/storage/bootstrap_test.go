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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseBootstrapWithBootstrapError(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	now := time.Now()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return now
	}))

	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Bootstrap(now, gomock.Any()).Return(fmt.Errorf("an error"))
	ns.EXPECT().ID().Return(ident.StringID("test"))
	namespaces := []databaseNamespace{ns}

	db := NewMockdatabase(ctrl)
	db.EXPECT().GetOwnedNamespaces().Return(namespaces, nil)

	m := NewMockdatabaseMediator(ctrl)
	m.EXPECT().DisableFileOps()
	m.EXPECT().EnableFileOps().AnyTimes()
	bsm := newBootstrapManager(db, m, opts).(*bootstrapManager)
	err := bsm.Bootstrap()

	require.NotNil(t, err)
	require.Equal(t, "an error", err.Error())
	require.Equal(t, Bootstrapped, bsm.state)
}

func TestDatabaseBootstrapSubsequentCallsQueued(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	now := time.Now()
	opts = opts.SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
		return now
	}))

	m := NewMockdatabaseMediator(ctrl)
	m.EXPECT().DisableFileOps()
	m.EXPECT().EnableFileOps().AnyTimes()

	db := NewMockdatabase(ctrl)

	bsm := newBootstrapManager(db, m, opts).(*bootstrapManager)

	ns := NewMockdatabaseNamespace(ctrl)

	var wg sync.WaitGroup
	wg.Add(1)
	ns.EXPECT().
		Bootstrap(now, gomock.Any()).
		Return(nil).
		Do(func(arg0, arg1 interface{}) {
			defer wg.Done()

			// Enqueue the second bootstrap
			err := bsm.Bootstrap()
			assert.Error(t, err)
			assert.Equal(t, errBootstrapEnqueued, err)
			assert.False(t, bsm.IsBootstrapped())
			bsm.RLock()
			assert.Equal(t, true, bsm.hasPending)
			bsm.RUnlock()

			// Expect the second bootstrap call
			ns.EXPECT().Bootstrap(now, gomock.Any()).Return(nil)
		})
	ns.EXPECT().
		ID().
		Return(ident.StringID("test")).
		Times(2)
	db.EXPECT().
		GetOwnedNamespaces().
		Return([]databaseNamespace{ns}, nil).
		Times(2)

	err := bsm.Bootstrap()
	require.Nil(t, err)
}
