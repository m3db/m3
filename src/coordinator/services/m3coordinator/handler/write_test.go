package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

func generatePromWriteRequest() *prompb.WriteRequest {
	req := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{
				{Name: "foo", Value: "bar"},
				{Name: "biz", Value: "baz"},
			},
			Samples: []*prompb.Sample{
				{Value: 1.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				{Value: 2.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
			},
		},
			{
				Labels: []*prompb.Label{
					{Name: "foo", Value: "qux"},
					{Name: "bar", Value: "baz"},
				},
				Samples: []*prompb.Sample{
					{Value: 3.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
					{Value: 4.0, Timestamp: time.Now().UnixNano() / int64(time.Millisecond)},
				},
			}},
	}
	return req
}

func generatePromWriteBody(t *testing.T) io.Reader {
	req := generatePromWriteRequest()
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}

	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)
	return b

}
func TestPromWriteParsing(t *testing.T) {
	logging.InitWithCores(nil)

	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promWrite := &PromWriteHandler{store: storage}

	req, _ := http.NewRequest("POST", PromWriteURL, generatePromWriteBody(t))

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Timeseries), 2)
}

func TestPromWrite(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	storage := local.NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promWrite := &PromWriteHandler{store: storage}

	req, _ := http.NewRequest("POST", PromWriteURL, generatePromWriteBody(t))

	r, err := promWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")

	writeErr := promWrite.write(context.TODO(), r)
	require.NoError(t, writeErr)
}
