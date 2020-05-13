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
	"github.com/m3db/m3/src/query/generated/proto/admin"
	dockertest "github.com/ory/dockertest"
)

const (
	defaultDBNodeSource     = "dbnode"
	defaultDBNodeName       = "dbnode01"
	defaultDBNodeDockerfile = "./m3dbnode.Dockerfile"
	defaultDBNodePort       = "9000/tcp"
)

var (
	defaultDBNodePortList = []int{2379, 2380, 9000, 9001, 9002, 9003, 9004}

	defaultDBNodeOptions = dockerResourceOptions{
		source:        defaultDBNodeSource,
		containerName: defaultDBNodeName,
		dockerFile:    defaultDBNodeDockerfile,
		defaultPort:   defaultDBNodePort,
		portList:      defaultDBNodePortList,
	}
)

// Node is a wrapper for a db node. It provides a wrapper on HTTP
// endpoints that expose cluster management APIs as well as read and write
// endpoints for series data.
// TODO: consider having this work on underlying structures.
type Node interface {
	HostDetails() (*admin.Host, error)
	Close() error
}

type dbNode struct {
	resource *dockerResource
}

func newDockerHTTPNode(
	pool *dockertest.Pool,
	opts dockerResourceOptions,
) (Node, error) {
	opts = opts.withDefaults(defaultDBNodeOptions)
	resource, err := newDockerResource(pool, opts)
	if err != nil {
		return nil, err
	}

	return &dbNode{
		resource: resource,
	}, nil
}

func (c *dbNode) HostDetails() (*admin.Host, error) {
	port, err := c.resource.getPort(9000)
	if err != nil {
		return nil, err
	}

	return &admin.Host{
		Id:             "m3db_local",
		IsolationGroup: "rack-a",
		Zone:           "embedded",
		Weight:         1024,
		Address:        defaultDBNodeName,
		Port:           uint32(port),
	}, nil
}

func (c *dbNode) Close() error {
	return c.resource.close()
}
