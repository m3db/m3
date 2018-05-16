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

package writer

import (
	"testing"
	"time"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"

	"github.com/stretchr/testify/require"
)

func TestOptions(t *testing.T) {
	opts := NewOptions()

	require.Empty(t, opts.TopicName())
	require.Equal(t, "topic", opts.SetTopicName("topic").TopicName())

	require.Equal(t, defaultTopicWatchInitTimeout, opts.TopicWatchInitTimeout())
	require.Equal(t, time.Second, opts.SetTopicWatchInitTimeout(time.Second).TopicWatchInitTimeout())

	require.Equal(t, defaultPlacementWatchInitTimeout, opts.PlacementWatchInitTimeout())
	require.Equal(t, time.Second, opts.SetPlacementWatchInitTimeout(time.Second).PlacementWatchInitTimeout())

	require.Equal(t, defaultMessageQueueScanInterval, opts.MessageQueueScanInterval())
	require.Equal(t, time.Second, opts.SetMessageQueueScanInterval(time.Second).MessageQueueScanInterval())

	require.Equal(t, defaultCloseCheckInterval, opts.CloseCheckInterval())
	require.Equal(t, time.Second, opts.SetCloseCheckInterval(time.Second).CloseCheckInterval())

	require.Equal(t, instrument.NewOptions(), opts.InstrumentOptions())
	require.Nil(t, opts.SetInstrumentOptions(nil).InstrumentOptions())
}

func TestConnectionOptions(t *testing.T) {
	opts := NewConnectionOptions()

	require.Equal(t, defaultDialTimeout, opts.DialTimeout())
	require.Equal(t, time.Second, opts.SetDialTimeout(time.Second).DialTimeout())

	require.Equal(t, defaultConnectionResetDelay, opts.ResetDelay())
	require.Equal(t, time.Second, opts.SetResetDelay(time.Second).ResetDelay())

	require.Equal(t, retry.NewOptions(), opts.RetryOptions())
	require.Nil(t, opts.SetRetryOptions(nil).RetryOptions())

	require.Equal(t, defaultConnectionBufferSize, opts.WriteBufferSize())
	require.Equal(t, 1, opts.SetWriteBufferSize(1).WriteBufferSize())

	require.Equal(t, defaultConnectionBufferSize, opts.ReadBufferSize())
	require.Equal(t, 1, opts.SetReadBufferSize(1).ReadBufferSize())

}
