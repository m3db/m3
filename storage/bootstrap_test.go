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

	"github.com/m3db/m3db/storage/bootstrap"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDatabaseBootstrapWithBootstrapError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	now := time.Now()
	cutover := now.Add(opts.GetRetentionOptions().GetBufferFuture())
	opts = opts.NewBootstrapFn(func() bootstrap.Bootstrap {
		return nil
	}).ClockOptions(opts.GetClockOptions().NowFn(func() time.Time {
		return now
	}))

	namespace := NewMockdatabaseNamespace(ctrl)
	namespace.EXPECT().Bootstrap(nil, now, cutover).Return(fmt.Errorf("an error"))

	namespaces := map[string]databaseNamespace{
		"test": namespace,
	}

	db := &mockDatabase{namespaces: namespaces, opts: opts}
	fsm := NewMockdatabaseFileSystemManager(ctrl)
	fsm.EXPECT().Run(now, false)
	bsm := newBootstrapManager(db, fsm).(*bootstrapManager)
	err := bsm.Bootstrap()

	require.NotNil(t, err)
	require.Equal(t, "an error", err.Error())
	require.Equal(t, bootstrapped, bsm.state)
}
