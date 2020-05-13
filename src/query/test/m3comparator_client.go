package test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
)

type m3comparatorClient struct {
	host string
	port int
}

func newM3ComparatorClient(host string, port int) *m3comparatorClient {
	return &m3comparatorClient{
		host: host,
		port: port,
	}
}

func (c *m3comparatorClient) clear() error {
	comparatorURL := fmt.Sprintf("http://%s:%d", c.host, c.port)
	req, err := http.NewRequest(http.MethodDelete, comparatorURL, nil)
	if err != nil {
		return err
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("could not clear data on m3comparator: %+v", err)
	}
	return nil
}

func (c *m3comparatorClient) load(data []byte) error {
	comparatorURL := fmt.Sprintf("http://%s:%d", c.host, c.port)
	resp, err := http.Post(comparatorURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("got error loading data to comparator %v", err)
	}

	if resp.StatusCode/200 != 1 {
		var bodyString string
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			bodyString = string(bodyBytes)
		}
		return fmt.Errorf("load status code %d. Response: %s", resp.StatusCode, bodyString)
	}

	return nil
}
