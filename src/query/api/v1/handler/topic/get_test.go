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
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3msg/generated/proto/topicpb"
	"github.com/m3db/m3msg/topic"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	jsonMarshaler   = jsonpb.Marshaler{EmitDefaults: true, Indent: "  "}
	jsonUnmarshaler = jsonpb.Unmarshaler{AllowUnknownFields: false}
)

func TestTopicGetHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockService := setupTest(t, ctrl)
	handler := NewGetHandler(nil, config.Configuration{})
	handler.serviceFn = testServiceFn(mockService)

	// Test successful get
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/topic/get", nil)
	require.NotNil(t, req)

	mockService.
		EXPECT().
		Get(gomock.Any()).
		Return(
			topic.NewTopic().
				SetName(DefaultTopicName).
				SetNumberOfShards(1024).
				SetVersion(2),
			nil,
		)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var respProto admin.TopicGetResponse
	require.NoError(t, jsonUnmarshaler.Unmarshal(bytes.NewBuffer(body), &respProto))

	validateEqualTopicProto(t, topicpb.Topic{
		Name:           DefaultTopicName,
		NumberOfShards: 1024,
	}, *respProto.Topic)
	require.Equal(t, uint32(2), respProto.Version)

	// Test error case
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/topic/get", nil)
	require.NotNil(t, req)

	mockService.EXPECT().Get(gomock.Any()).Return(nil, errors.New("key not found"))
	handler.ServeHTTP(w, req)

	resp = w.Result()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func validateEqualTopicProto(t *testing.T, this, other topicpb.Topic) {
	t1, err := topic.NewTopicFromProto(&this)
	require.NoError(t, err)
	t2, err := topic.NewTopicFromProto(&other)
	require.NoError(t, err)
	require.Equal(t, t1, t2)
}

func testServiceFn(s topic.Service) serviceFn {
	return func(clusterClient clusterclient.Client) (topic.Service, error) {
		return s, nil
	}
}

func setupTest(t *testing.T, ctrl *gomock.Controller) *topic.MockService {
	logging.InitWithCores(nil)
	s := topic.NewMockService(ctrl)
	return s
}
