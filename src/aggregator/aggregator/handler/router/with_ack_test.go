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

package router

import (
	"testing"

	"github.com/m3db/m3/src/aggregator/aggregator/handler/common"
	"github.com/m3db/m3metrics/encoding/msgpack"
	"github.com/m3db/m3msg/producer"

	"github.com/stretchr/testify/require"
)

func TestWithAckRouterDecRefBuffer(t *testing.T) {
	buf := common.NewRefCountedBuffer(msgpack.NewPooledBufferedEncoderSize(nil, 1024))
	msg := newMessage(2, buf)
	require.Equal(t, uint32(2), msg.Shard())
	require.Equal(t, 1024, msg.Size())
	require.Empty(t, msg.Bytes())

	msg.Finalize(producer.Consumed)
	require.Panics(t, buf.DecRef)
}
