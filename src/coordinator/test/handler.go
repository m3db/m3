package test

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3db/src/coordinator/generated/proto/prompb"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
)

// SlowHandler slows down a request by delay
type SlowHandler struct {
	handler http.Handler
	delay   time.Duration
}

// NewSlowHandler creates a new slow handler
func NewSlowHandler(handler http.Handler, delay time.Duration) *SlowHandler {
	return &SlowHandler{handler: handler, delay: delay}
}

// ServeHTTP implements http.handler
func (h *SlowHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(h.delay)
	h.handler.ServeHTTP(w, r)
}

func GeneratePromReadRequest() *prompb.ReadRequest {
	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			Matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "eq", Value: "a"},
			},
			StartTimestampMs: time.Now().Add(-1*time.Hour*24).UnixNano() / int64(time.Millisecond),
			EndTimestampMs:   time.Now().UnixNano() / int64(time.Millisecond),
		}},
	}
	return req
}

func GeneratePromReadBody(t *testing.T) io.Reader {
	req := GeneratePromReadRequest()
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
