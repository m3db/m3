package handler

import (
	"io/ioutil"
	"github.com/golang/snappy"
	"net/http"
	"github.com/m3db/m3coordinator/generated/proto/prometheus/prompb"
	"github.com/golang/protobuf/proto"
	"fmt"
)

// PromReadHandler represents a handler for prometheus read endpoint.
type PromReadHandler struct {
}

// NewPromReadHandler returns a new instance of handler.
func NewPromReadHandler() http.Handler {
	return &PromReadHandler{}
}

func (h *PromReadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, err := h.parseRequest(w, r)
	if err != nil {
		return
	}

	// TODO: Actual read
	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}

	data, err := proto.Marshal(resp)
	if err != nil {
		Error(w, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Encoding", "snappy")

	compressed := snappy.Encode(nil, data)
	if _, err := w.Write(compressed); err != nil {
		Error(w, err, http.StatusInternalServerError)
		return
	}
}
func (h *PromReadHandler) parseRequest(w http.ResponseWriter, r *http.Request) (*prompb.ReadRequest, error) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		Error(w, err, http.StatusInternalServerError)
		return nil, err
	}

	if len(compressed) == 0 {
		Error(w, fmt.Errorf("empty request body"), http.StatusBadRequest)
		return nil, err
	}

	fmt.Println(compressed, "compressed")
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		Error(w, err, http.StatusBadRequest)
		return nil, err
	}

	var req prompb.ReadRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		Error(w, err, http.StatusBadRequest)
		return nil, err
	}
	return &req, nil

}
