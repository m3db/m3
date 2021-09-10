package promremotewrite

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	tcs := []struct {
		name       string
		tags       []models.Tag
		datapoints ts.Datapoints

		expectedLabels  []prompb.Label
		expectedSamples []prompb.Sample
	}{
		{
			name: "write single datapoint with labels",
			tags: []models.Tag{{
				Name:  []byte("test_tag_name"),
				Value: []byte("test_tag_value"),
			}},
			datapoints: ts.Datapoints{{
				Timestamp: xtime.UnixNano(time.Second),
				Value:     42,
			}},

			expectedLabels: []prompb.Label{{
				Name:  "test_tag_name",
				Value: "test_tag_value",
			}},
			expectedSamples: []prompb.Sample{{
				Value:     42,
				Timestamp: int64(1000),
			}},
		},
	}

	server, closeFn := newFakePromRemoteWriteServer(t)
	defer closeFn()

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			appender, err := NewAppender(Options{endpoint: fmt.Sprintf("http://%s", server.Addr())})
			require.NoError(t, err)

			wq, err := storage.NewWriteQuery(storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions(),
					Tags: tc.tags,
				},
				Datapoints: tc.datapoints,
				// TODO what is the meaning of this?
				Unit: xtime.Millisecond,
			})
			require.NoError(t, err)
			err = appender.Write(context.TODO(), wq)
			require.NoError(t, err)

			promWrite := server.lastWriteRequest
			require.Len(t, promWrite.Timeseries, 1)
			require.Len(t, promWrite.Timeseries[0].Labels, len(tc.expectedLabels))
			require.Len(t, promWrite.Timeseries[0].Samples, len(tc.expectedSamples))

			for i := 0; i < len(tc.expectedLabels); i++ {
				assert.Equal(t, promWrite.Timeseries[0].Labels[i], tc.expectedLabels[i])
			}
			for i := 0; i < len(tc.expectedSamples); i++ {
				assert.Equal(t, promWrite.Timeseries[0].Samples[i], tc.expectedSamples[i])
			}
			assertRemoteWriteHeadersSetCorrectly(t, server.lastHTTPRequest)
		})
	}

}

func assertRemoteWriteHeadersSetCorrectly(t *testing.T, r *http.Request) {
	assert.Equal(t, r.Header.Get("content-encoding"), "snappy")
	assert.Equal(t, r.Header.Get("content-type"), "application/x-protobuf")
}

type fakePromRemoteWriteServer struct {
	lastWriteRequest *prompb.WriteRequest
	lastHTTPRequest *http.Request
	addr             string
}

func newFakePromRemoteWriteServer(t *testing.T) (*fakePromRemoteWriteServer, func()) {
	server := &fakePromRemoteWriteServer{}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	go func() {
		err = http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			server.lastHTTPRequest = request
			req, err := remote.DecodeWriteRequest(request.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			server.lastWriteRequest = req
		}))
		if err != nil {
			require.NoError(t, err)
		}
	}()
	server.addr = listener.Addr().String()
	return server, func() { _ = listener.Close() }
}

func (s *fakePromRemoteWriteServer) getLastRequest() *prompb.WriteRequest {
	return s.lastWriteRequest
}

func (s *fakePromRemoteWriteServer) Addr() string {
	return s.addr
}
