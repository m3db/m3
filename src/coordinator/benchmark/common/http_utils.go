package common

import (
	"io"
	"net/http"
)

// PostEncodedSnappy wraps request with content headers then POSTs to url with given body
func PostEncodedSnappy(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")

	client := http.DefaultClient
	return client.Do(req)
}
