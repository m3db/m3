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
	"testing"
	"time"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"
	"github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDatabaseBootstrapWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	now := time.Now()
	cutover := now.Add(opts.GetBufferFuture())
	bs := mocks.NewMockBootstrap(ctrl)
	opts = opts.NewBootstrapFn(func() m3db.Bootstrap { return bs }).NowFn(func() time.Time { return now })

	errs := []error{
		errors.New("some error"),
		errors.New("some other error"),
		nil,
	}

	var shards []databaseShard
	for _, err := range errs {
		shard := mocks.NewMockdatabaseShard(ctrl)
		shard.EXPECT().Bootstrap(bs, now, cutover).Return(err)
		shards = append(shards, shard)
	}

	db := &mockDatabase{shards: shards, opts: opts}
	bsm := newBootstrapManager(db).(*bootstrapManager)
	err := bsm.Bootstrap()

	require.NotNil(t, err)
	require.Equal(t, "some error\nsome other error", err.Error())
	require.Equal(t, bootstrapped, bsm.state)
}
