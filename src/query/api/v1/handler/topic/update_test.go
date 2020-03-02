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
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestPlacementUpdateHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockService := setupTest(t, ctrl)
	handler := NewUpdateHandler(nil, config.Configuration{}, instrument.NewOptions())
	handler.serviceFn = testServiceFn(mockService)

	// Test topic init success
	updateProto := admin.TopicUpdateRequest{
		ConsumerServices: []*topicpb.ConsumerService{
			{
				ServiceId: &topicpb.ServiceID{
					Name:        "svc",
					Environment: "env",
					Zone:        "zone",
				},
				ConsumptionType: topicpb.ConsumptionType_SHARED,
			},
		},
		Version: 2,
	}
	w := httptest.NewRecorder()
	b := bytes.NewBuffer(nil)
	require.NoError(t, jsonMarshaler.Marshal(b, &updateProto))
	req := httptest.NewRequest("PUT", "/topic/update", b)
	req.Header.Add("topic-name", "test-topic")
	require.NotNil(t, req)

	returnTopic := topic.NewTopic().
		SetName("test-topic").
		SetNumberOfShards(256).
		SetVersion(1)

	mockService.EXPECT().
		Get("test-topic").
		Return(returnTopic, nil)

	mockService.
		EXPECT().
		CheckAndSet(returnTopic, 2).
		Return(
			returnTopic.SetVersion(3),
			nil,
		)

	handler.ServeHTTP(w, req)
	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var respProto admin.TopicGetResponse
	require.NoError(t, jsonUnmarshaler.Unmarshal(bytes.NewBuffer(body), &respProto))

	validateEqualTopicProto(t, topicpb.Topic{
		Name:           "test-topic",
		NumberOfShards: 256,
	}, *respProto.Topic)

	require.Equal(t, uint32(3), respProto.Version)
}
