package promremotewrite

import (
	"context"
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
	server, closeFn := newFakePromRemoteWriteServer(t)
	defer closeFn()

	appender, err := NewAppender(Options{endpoint: "http://" + server.Addr() + "/"})
	require.NoError(t, err)

	wq, err := storage.NewWriteQuery(storage.WriteQueryOptions{
		Tags: models.Tags{
			Opts: models.NewTagOptions(),
			Tags: []models.Tag{{
				Name:  []byte("test_tag_name"),
				Value: []byte("test_tag_value"),
			}},
		},
		Datapoints: ts.Datapoints{{
			Timestamp: xtime.UnixNano(time.Second),
			Value:     42,
		}},
		// TODO what is the meaning of this?
		Unit: xtime.Millisecond,
	})
	require.NoError(t, err)
	err = appender.Write(context.TODO(), wq)
	require.NoError(t, err)

	promWrite := server.lastWriteRequest
	require.Len(t, promWrite.Timeseries, 1)
	require.Len(t, promWrite.Timeseries[0].Labels, 1)
	require.Len(t, promWrite.Timeseries[0].Samples, 1)
	assert.Equal(t, promWrite.Timeseries[0].Labels[0], prompb.Label{
		Name:  "test_tag_name",
		Value: "test_tag_value",
	})
	assert.Equal(t, promWrite.Timeseries[0].Samples[0], prompb.Sample{
		Value:     42,
		Timestamp: int64(1000),
	})
}

type fakePromRemoteWriteServer struct {
	lastWriteRequest *prompb.WriteRequest
	addr             string
}

func newFakePromRemoteWriteServer(t *testing.T) (*fakePromRemoteWriteServer, func()) {
	server := &fakePromRemoteWriteServer{}
	listener, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	go func() {
		err = http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
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
	time.Sleep(1 *time.Second)
	server.addr = listener.Addr().String()
	return server, func() { _ = listener.Close() }
}

func (s *fakePromRemoteWriteServer) getLastRequest() *prompb.WriteRequest {
	return s.lastWriteRequest
}

func (s *fakePromRemoteWriteServer) Addr() string {
	return s.addr
}
