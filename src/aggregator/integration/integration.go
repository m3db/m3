// Copyright (c) 2016 Uber Technologies, Inc.
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

// Package integration contains integration tests for aggregator.
package integration

import (
	"fmt"
	"os"
	"strings"
	"time"

	aggclient "github.com/m3db/m3/src/aggregator/client"
)

type conditionFn func() bool

func waitUntil(fn conditionFn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func getAggregatorClientTypeFromEnv() (aggclient.AggregatorClientType, error) {
	clientType := strings.ToLower(os.Getenv("TEST_AGGREGATOR_CLIENT_TYPE"))
	switch clientType {
	case "", "tcp":
		return aggclient.TCPAggregatorClient, nil
	case "m3msg":
		return aggclient.M3MsgAggregatorClient, nil
	default:
		return aggclient.AggregatorClientType(0), fmt.Errorf("unrecognized aggregator client type %v", clientType)
	}
}
