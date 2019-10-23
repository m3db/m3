// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

func paramError(err string, log *zap.Logger) {
	log.Error(err)
	flag.Usage()
}

func main() {
	var (
		iOpts = instrument.NewOptions()
		log   = iOpts.Logger()

		now = time.Now()

		pQ = flag.String("query", "rate(quail[5m]):15s",
			"query for comparison, delimited with step size by `:`")
		pPromAddress  = flag.String("promAdress", "0.0.0.0:9090", "prom address")
		pQueryAddress = flag.String("queryAddress", "0.0.0.0:7201", "query address")

		pStart   = flag.Int64("s", now.Add(time.Hour*-3).Unix(), "start time")
		pEnd     = flag.Int64("e", now.Unix(), "start end")
		pRetries = flag.Int64("r", 5, "retries")
	)

	flag.Parse()
	var (
		query        = strings.Trim(*pQ, `"`)
		promAddress  = *pPromAddress
		queryAddress = *pQueryAddress

		start = *pStart
		end   = *pEnd
	)

	if len(query) == 0 {
		paramError("No query found", log)
		os.Exit(1)
	}

	splitQuery := strings.Split(query, ":")
	if len(splitQuery) != 2 {
		paramError("Query has no delimiter", log)
		os.Exit(1)
	}

	query = url.QueryEscape(splitQuery[0])
	step := splitQuery[1]

	if len(promAddress) == 0 {
		paramError("No prom address found", log)
		os.Exit(1)
	}

	if len(queryAddress) == 0 {
		paramError("No query server address found", log)
		os.Exit(1)
	}

	if end < start {
		paramError(fmt.Sprintf("start(%d) is before end (%d)", start, end), log)
		os.Exit(1)
	}

	url := fmt.Sprintf(
		"/api/v1/query_range?query=%s&start=%d&end=%d&step=%s",
		query, start, end, step,
	)

	promURL := fmt.Sprintf("http://%s%s", promAddress, url)
	queryURL := fmt.Sprintf("http://%s%s", queryAddress, url)

	retries := int(*pRetries)
	for i := 0; i < retries; i++ {
		err := runComparison(promURL, queryURL, log)
		if err == nil {
			return
		}

		time.Sleep(time.Second)
		log.Info(
			"retrying query",
			zap.String("promURL", promURL),
			zap.String("queryURL", queryURL),
			zap.Int("attempt", i),
		)
	}

	os.Exit(1)
}

func runComparison(
	promURL string,
	queryURL string,
	log *zap.Logger,
) error {
	promResult, err := parseResult(promURL, log)
	if err != nil {
		log.Error("Could not parse prometheus result", zap.Error(err))
		return err
	}

	queryResult, err := parseResult(queryURL, log)
	if err != nil {
		log.Error("Could not parse m3query result", zap.Error(err))
		return err
	}

	_, err = promResult.Matches(queryResult)
	if err != nil {
		log.Error("results do not match", zap.Error((err)))
		return err
	}

	return nil
}

func parseResult(
	endpoint string,
	log *zap.Logger,
) (prometheus.Response, error) {
	var result prometheus.Response
	response, err := http.Get(endpoint)
	if err != nil {
		return result, err
	}

	if response.StatusCode != http.StatusOK {
		return result, fmt.Errorf("response failed with code %s", response.Status)
	}

	body := response.Body
	defer func() {
		body.Close()
	}()

	data, err := ioutil.ReadAll(body)
	if err != nil {
		return result, err
	}

	if err = json.Unmarshal(data, &result); err != nil {
		return result, err
	}

	return result, nil
}
