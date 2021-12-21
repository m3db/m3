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

package config

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"
	xio "github.com/m3db/m3/src/x/io"
)

func TestConnectionConfiguration(t *testing.T) {
	str := `
dialTimeout: 3s
writeTimeout: 2s
keepAlivePeriod: 20s
resetDelay: 1s
retry:
  initialBackoff: 1ms
  maxBackoff: 2ms
flushInterval: 2s
writeBufferSize: 100
readBufferSize: 200
`

	var cfg ConnectionConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	cOpts := cfg.NewOptions(instrument.NewOptions())
	require.Equal(t, 3*time.Second, cOpts.DialTimeout())
	require.Equal(t, 2*time.Second, cOpts.WriteTimeout())
	require.Equal(t, 20*time.Second, cOpts.KeepAlivePeriod())
	require.Equal(t, time.Second, cOpts.ResetDelay())
	require.Equal(t, time.Millisecond, cOpts.RetryOptions().InitialBackoff())
	require.Equal(t, 2*time.Millisecond, cOpts.RetryOptions().MaxBackoff())
	require.Equal(t, 2*time.Second, cOpts.FlushInterval())
	require.Equal(t, 100, cOpts.WriteBufferSize())
	require.Equal(t, 200, cOpts.ReadBufferSize())
}

func TestWriterConfiguration(t *testing.T) {
	str := `
topicName: testTopic
topicServiceOverride:
  zone: z1
  namespace: n1
topicWatchInitTimeout: 1s
placementServiceOverride:
  namespaces:
    placement: n2
placementWatchInitTimeout: 2s
messagePool:
  size: 5
messageRetry:
  initialBackoff: 1ms
messageQueueNewWritesScanInterval: 200ms
messageQueueFullScanInterval: 10s
messageQueueScanBatchSize: 1024
initialAckMapSize: 1024
closeCheckInterval: 2s
ackErrorRetry:
  initialBackoff: 2ms
connection:
  dialTimeout: 5s
encoder:
  maxMessageSize: 100
decoder:
  maxMessageSize: 200
`
	var cfg WriterConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(str), &cfg))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	cs := client.NewMockClient(ctrl)
	cs.EXPECT().Store(kv.NewOverrideOptions().SetZone("z1").SetNamespace("n1")).Return(nil, nil)
	cs.EXPECT().Services(
		services.NewOverrideOptions().SetNamespaceOptions(
			services.NewNamespaceOptions().SetPlacementNamespace("n2"),
		),
	).Return(nil, nil)

	wOpts, err := cfg.NewOptions(cs, instrument.NewOptions(), xio.NewOptions())
	require.NoError(t, err)
	require.Equal(t, "testTopic", wOpts.TopicName())
	require.Equal(t, time.Second, wOpts.TopicWatchInitTimeout())
	require.Equal(t, 2*time.Second, wOpts.PlacementWatchInitTimeout())
	require.Equal(t, 5, wOpts.MessagePoolOptions().Size())
	require.NotNil(t, wOpts.MessageRetryNanosFn())
	require.Equal(t, 200*time.Millisecond, wOpts.MessageQueueNewWritesScanInterval())
	require.Equal(t, 10*time.Second, wOpts.MessageQueueFullScanInterval())
	require.Equal(t, 1024, wOpts.MessageQueueScanBatchSize())
	require.Equal(t, 1024, wOpts.InitialAckMapSize())
	require.Equal(t, 2*time.Second, wOpts.CloseCheckInterval())
	require.Equal(t, 2*time.Millisecond, wOpts.AckErrorRetryOptions().InitialBackoff())
	require.Equal(t, 5*time.Second, wOpts.ConnectionOptions().DialTimeout())
	require.Equal(t, 100, wOpts.EncoderOptions().MaxMessageSize())
	require.Equal(t, 200, wOpts.DecoderOptions().MaxMessageSize())
}
