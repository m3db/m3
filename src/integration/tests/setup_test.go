// Copyright (c) 2021  Uber Technologies, Inc.
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

package tests

import (
	"fmt"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/m3db/m3/src/integration/resources/inprocess"

	"github.com/m3db/m3/src/integration/resources"
)

var singleDBNodeDockerResources resources.M3Resources

func TestMain(m *testing.M) {
	_, filename, _, _ := runtime.Caller(0)
	pathToDBCfg := path.Join(path.Dir(filename), "m3dbnode.yml")
	pathToCoordCfg := path.Join(path.Dir(filename), "m3coordinator.yml")

	cfgs, err := inprocess.NewClusterConfigsFromConfigFile(pathToDBCfg, pathToCoordCfg)
	if err != nil {
		fmt.Println("could not create cluster configs", err)
		terminate(1)
	}

	// Spin up M3 resources
	singleDBNodeDockerResources, err = inprocess.NewCluster(cfgs,
		inprocess.ClusterOptions{
			DBNode: inprocess.NewDBNodeClusterOptions(),
		},
	)
	if err != nil {
		fmt.Println("could not set up m3 cluster", err)
		terminate(1)
	}

	code := m.Run()
	os.Exit(code)
}

func terminate(exitCode int) {
	if singleDBNodeDockerResources != nil {
		if err := singleDBNodeDockerResources.Cleanup(); err != nil {
			fmt.Println("error during M3 resource cleanup. " +
				"shutdown may not be graceful")
		}
	}

	os.Exit(exitCode)
}
