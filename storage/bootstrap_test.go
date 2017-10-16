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
	"testing"
	"time"

	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabaseBootstrapWithBootstrapError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	now := time.Now()
	opts = opts.
		SetBootstrapProcess(nil).
		SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
			return now
		}))

	ns := NewMockdatabaseNamespace(ctrl)
	ns.EXPECT().Options().Return(namespace.NewOptions())
	ns.EXPECT().Bootstrap(nil, gomock.Any()).Return(fmt.Errorf("an error"))
	ns.EXPECT().ID().Return(ts.StringID("test"))
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
	require.Equal(t, bootstrapped, bsm.state)
}

func TestDatabaseBootstrapTargetRanges(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ns, err := namespace.NewMetadata(defaultTestNs1ID, defaultTestNs1Opts)
	require.NoError(t, err)
	ropts := ns.Options().RetentionOptions()
	now := time.Now().Truncate(ropts.BlockSize()).Add(8 * time.Minute)
	opts = opts.
		SetBootstrapProcess(nil).
		SetClockOptions(opts.ClockOptions().SetNowFn(func() time.Time {
			return now
		}))

	db := NewMockdatabase(ctrl)
	bsm := newBootstrapManager(db, nil, opts).(*bootstrapManager)
	ranges := bsm.targetRanges(now, ropts)

	var all [][]time.Time
	for _, target := range ranges {
		value := target.Range
		var times []time.Time
		for st := value.Start; st.Before(value.End); st = st.Add(ropts.BlockSize()) {
			times = append(times, st)
		}
		all = append(all, times)
	}

	require.Equal(t, 2, len(all))

	firstWindowExpected :=
		int(ropts.RetentionPeriod()/ropts.BlockSize()) - 1
	secondWindowExpected := 2

	assert.Equal(t, firstWindowExpected, len(all[0]))
	assert.True(t, all[0][0].Equal(now.Truncate(ropts.BlockSize()).Add(-1*ropts.RetentionPeriod())))
	assert.True(t, all[0][firstWindowExpected-1].Equal(now.Truncate(ropts.BlockSize()).Add(-2*ropts.BlockSize())))

	require.Equal(t, secondWindowExpected, len(all[1]))
	assert.True(t, all[1][0].Equal(now.Truncate(ropts.BlockSize()).Add(-1*ropts.BlockSize())))
	assert.True(t, all[1][1].Equal(now.Truncate(ropts.BlockSize())))
}
