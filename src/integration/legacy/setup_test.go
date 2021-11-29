// Copyright (c) 2020  Uber Technologies, Inc.
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

package legacy

import (
	"fmt"
	"os"
	"testing"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/integration/resources/inprocess"
)

var m3 resources.M3Resources

//nolint:forbidigo
func TestMain(m *testing.M) {
	configs, err := inprocess.NewClusterConfigsFromYAML(dbCfg, coordCfg, "")
	if err != nil {
		fmt.Println("could not create m3 cluster configs")
		os.Exit(1)
	}

	m3, err = inprocess.NewCluster(configs, resources.ClusterOptions{
		DBNode: resources.NewDBNodeClusterOptions(),
	})
	if err != nil {
		fmt.Println("could not set up m3 cluster", err)
		os.Exit(1)
	}

	if err = m3.Nodes().WaitForHealthy(); err != nil {
		fmt.Println("error checking cluster for health", err)
		os.Exit(1)
	}

	code := m.Run()

	if err = m3.Cleanup(); err != nil {
		fmt.Println("error cleaning up M3. may not have closed gracefully")
	}

	os.Exit(code)
}

const (
	dbCfg    = `db: {}`
	coordCfg = `
clusters:
  - namespaces:
      - namespace: aggregated
        type: aggregated
        retention: 10h
        resolution: 5s
      - namespace: default
        type: unaggregated
        retention: 48h

carbon:
  ingester:
    listenAddress: "0.0.0.0:7204"
    rules:
      - pattern: .*min.aggregate.*
        aggregation:
          type: min
        policies:
          - resolution: 5s
            retention: 10h
      - pattern: .*already-aggregated.*
        aggregation:
          enabled: false
        policies:
          - resolution: 5s
            retention: 10h
      - pattern: .*
        policies:
          - resolution: 5s
            retention: 10h
`
)
