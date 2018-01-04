package handler

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/golang/snappy"
)

// ParsePromRequest parses a snappy compressed request from Prometheus
func ParsePromRequest(r *http.Request) ([]byte, *ParseError) {
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewParseError(err, http.StatusInternalServerError)
	}

	if len(compressed) == 0 {
		return nil, NewParseError(fmt.Errorf("empty request body"), http.StatusBadRequest)
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return reqBuf, nil
}
