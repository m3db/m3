package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/m3db/m3coordinator/mocks"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/tsdb"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generatePromReadRequest() *prompb.ReadRequest {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "eq", Value: "a"},
			},
			StartTimestampMs: time.Now().Add(-1 * time.Hour * 24).UnixNano() / int64(time.Millisecond),
			EndTimestampMs:   time.Now().UnixNano() / int64(time.Millisecond),
		}},
	}
	return req
}

func generatePromReadBody(t *testing.T) io.Reader {
	req := generatePromReadRequest()
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}

	compressed := snappy.Encode(nil, data)
	// Uncomment the line below to write the data into a file useful for integration testing
	//ioutil.WriteFile("/tmp/dat1", compressed, 0644)
	b := bytes.NewReader(compressed)
	return b

}

func setupServer(t *testing.T) *httptest.Server{
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	// No calls expected on session object
	session := client.NewMockSession(ctrl)
	mockResolver := mocks.NewMockPolicyResolver(gomock.NewController(t))

	mockResolver.EXPECT().Resolve(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Do(func(_, _, _, _ interface{}) {
		time.Sleep(10 * time.Millisecond)
	}).Return([]tsdb.FetchRequest{}, nil)

	storage := local.NewStorage(session, "metrics", mockResolver)
	engine := executor.NewEngine(storage)
	promRead := &PromReadHandler{engine: engine}

	server := httptest.NewServer(promRead)
	return server
}

func TestPromReadParsing(t *testing.T) {
	logging.InitWithCores(nil)
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("POST", PromReadURL, generatePromReadBody(t))

	r, err := promRead.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Queries), 1)
}

func TestPromReadParsingBad(t *testing.T) {
	logging.InitWithCores(nil)
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req, _ := http.NewRequest("POST", PromReadURL, strings.NewReader("bad body"))
	_, err := promRead.parseRequest(req)
	require.NotNil(t, err, "unable to parse request")
}

func TestPromReadStorageWithFetchError(t *testing.T) {
	logging.InitWithCores(nil)
	ctrl := gomock.NewController(t)
	session := client.NewMockSession(ctrl)
	session.EXPECT().Fetch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("unable to get data"))

	storage := local.NewStorage(session, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))
	promRead := &PromReadHandler{engine: executor.NewEngine(storage)}
	req := generatePromReadRequest()
	_, err := promRead.read(context.TODO(), httptest.NewRecorder(), req, &RequestParams{Timeout: time.Hour})
	require.NotNil(t, err, "unable to read from storage")
}

func TestQueryMatchMustBeEqual(t *testing.T) {
	logging.InitWithCores(nil)

	req := generatePromReadRequest()
	matchers, err := storage.PromMatchersToM3(req.Queries[0].Matchers)
	require.NoError(t, err)

	_, err = matchers.ToTags()
	assert.NoError(t, err)
}

func TestQueryKillOnClientDisconnect(t *testing.T) {
	server := setupServer(t)
	defer server.Close()

	c := &http.Client{
		Timeout: 1 * time.Millisecond,
	}

	_, err := c.Post(server.URL, "application/x-protobuf", generatePromReadBody(t))
	assert.Error(t, err)
}

func TestQueryKillOnTimeout(t *testing.T) {
	server := setupServer(t)
	defer server.Close()
	
	req, _ := http.NewRequest("POST", server.URL, generatePromReadBody(t))
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("timeout", "1ms")
	resp, _ := http.DefaultClient.Do(req)
	defer resp.Body.Close()
	require.NotNil(t, resp)
	assert.Equal(t, resp.StatusCode, 500, "Status code not 500")
}
