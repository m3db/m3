// Copyright (c) 2019 Uber Technologies, Inc.
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

	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/msg/generated/proto/topicpb"
	"github.com/m3db/m3/src/msg/topic"
)

var (
	jsonMarshaler   = jsonpb.Marshaler{EmitDefaults: true, Indent: "  "}
	jsonUnmarshaler = jsonpb.Unmarshaler{AllowUnknownFields: false}
)

func validateEqualTopicProto(t *testing.T, this, other topicpb.Topic) {
	t1, err := topic.NewTopicFromProto(&this)
	require.NoError(t, err)
	t2, err := topic.NewTopicFromProto(&other)
	require.NoError(t, err)
	require.Equal(t, t1, t2)
}

func testServiceFn(s topic.Service) serviceFn {
	return func(clusterClient clusterclient.Client, opts handleroptions.ServiceOptions) (topic.Service, error) {
		return s, nil
	}
}

func setupTest(t *testing.T, ctrl *gomock.Controller) *topic.MockService {
	return topic.NewMockService(ctrl)
}
