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

package topic

import (
	"testing"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/msg/generated/proto/msgpb"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestTopicService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kvOpts := kv.NewOverrideOptions().SetNamespace("foo")
	cs := client.NewMockClient(ctrl)
	store := mem.NewStore()
	cs.EXPECT().Store(kvOpts).Return(store, nil)

	s, err := NewService(NewServiceOptions().SetConfigService(cs).SetKVOverrideOptions(kvOpts))
	require.NoError(t, err)

	topicName := "topic1"
	_, err = s.Get(topicName)
	require.Error(t, err)

	_, err = s.CheckAndSet(NewTopic(), kv.UninitializedVersion)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid topic")

	w, err := s.Watch(topicName)
	require.NoError(t, err)
	require.Equal(t, 0, len(w.C()))

	topic1 := NewTopic().
		SetName(topicName).
		SetNumberOfShards(100).
		SetConsumerServices([]ConsumerService{
			NewConsumerService().SetConsumptionType(Shared).SetServiceID(services.NewServiceID().SetName("s1")),
			NewConsumerService().SetConsumptionType(Replicated).SetServiceID(services.NewServiceID().SetName("s2")),
		})
	topic1, err = s.CheckAndSet(topic1, kv.UninitializedVersion)
	require.NoError(t, err)
	topic1, err = s.CheckAndSet(topic1, 1)
	require.NoError(t, err)

	topic2, err := s.Get(topicName)
	require.NoError(t, err)
	require.Equal(t, 2, topic2.Version())
	require.Equal(t, topic1.Name(), topic2.Name())
	require.Equal(t, topic1.NumberOfShards(), topic2.NumberOfShards())
	require.Equal(t, topic1.ConsumerServices(), topic2.ConsumerServices())

	<-w.C()
	topic, err := w.Get()
	require.NoError(t, err)
	require.Equal(t, topic2, topic)

	err = s.Delete(topicName)
	require.NoError(t, err)

	<-w.C()
	_, err = w.Get()
	require.Error(t, err)

	_, err = s.CheckAndSet(topic1, kv.UninitializedVersion)
	require.NoError(t, err)

	<-w.C()
	topic, err = w.Get()
	require.NoError(t, err)

	topic3, err := s.Get(topicName)
	require.NoError(t, err)
	require.Equal(t, 1, topic3.Version())
	require.Equal(t, topic3, topic)

	version, err := store.Set(key(topicName), &msgpb.Message{Value: []byte("bad proto")})
	require.NoError(t, err)
	require.Equal(t, 2, version)

	_, err = s.Get(topicName)
	require.Error(t, err)

	<-w.C()
	_, err = w.Get()
	require.Error(t, err)

	w.Close()
}
