package test

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type m3queryClient struct {
	host string
	port int
}

func newM3QueryClient(host string, port int) *m3queryClient {
	return &m3queryClient{
		host: host,
		port: port,
	}
}

func (c *m3queryClient) query(expr string, t time.Time) ([]byte, error) {
	url := fmt.Sprintf("http://%s:%d/api/v1/query", c.host, c.port)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("query", expr)
	q.Add("time", strconv.FormatInt(t.Unix(), 10))
	req.URL.RawQuery = q.Encode()
	fmt.Printf("Requesting m3query URL: %+v\n", req.URL)
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return nil, errors.Wrapf(err, "error evaluating query %s", expr)
	}

	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status %+v received sending query: %+v", resp.StatusCode, req)
	}

	return ioutil.ReadAll(resp.Body)
}
