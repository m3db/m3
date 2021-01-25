// +build dtest
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package harness

import (
	"fmt"
	"os"
	"testing"

	"github.com/m3db/m3/src/cmd/tools/dtest/docker/harness/resources"
)

var singleDBNodeDockerResources resources.DockerResources

func TestMain(m *testing.M) {
	var err error
	singleDBNodeDockerResources, err = resources.SetupSingleM3DBNode(
		resources.WithExistingCluster("dbnode01", "coord01"),
	)

	if err != nil {
		fmt.Println("could not set up db docker containers", err)
		os.Exit(1)
	}

	if l := len(singleDBNodeDockerResources.Nodes()); l != 1 {
		fmt.Println("should only have a single node, have", l)
		os.Exit(1)
	}

	code := m.Run()
	os.Exit(code)
}
