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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/m3db/m3/src/dbnode/generated/thrift/rpc"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	dockertest "github.com/ory/dockertest"
	"github.com/ory/dockertest/docker"
	"go.uber.org/zap"
)

const (
	defaultDBNodeSource     = "dbnode"
	defaultDBNodeName       = "dbnode01"
	defaultDBNodeDockerfile = "./m3dbnode.Dockerfile"
)

var (
	defaultDBNodePortList = []int{2379, 2380, 9000, 9001, 9002, 9003, 9004}

	defaultDBNodeOptions = dockerResourceOptions{
		source:        defaultDBNodeSource,
		containerName: defaultDBNodeName,
		dockerFile:    defaultDBNodeDockerfile,
		portList:      defaultDBNodePortList,
	}
)

// Node is a wrapper for a db node. It provides a wrapper on HTTP
// endpoints that expose cluster management APIs as well as read and write
// endpoints for series data.
// TODO: consider having this work on underlying structures.
type Node interface {
	// HostDetails returns this node's host details.
	HostDetails() (*admin.Host, error)
	// WaitForBootstrap blocks until the node has bootstrapped.
	WaitForBootstrap() error
	// WritePoint writes a datapoint to the node directly.
	WritePoint(rpc.WriteRequest) error
	// Fetch fetches datapoints.
	Fetch(rpc.FetchRequest) (rpc.FetchResult_, error)
	// CheckForCheckpoint checks for a checkpointed file.
	CheckForCheckpoint() error
	// Restart restarts this container.
	Restart() error
	// Close closes the wrapper and releases any held resources, including
	// deleting docker containers.
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
	opts.mounts = []string{setupMount("/etc/m3coordinator/")}

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

func (c *dbNode) Health() (rpc.NodeHealthResult_, error) {
	if c.resource.closed {
		return rpc.NodeHealthResult_{}, errClosed
	}

	url := c.resource.getURL(9002, "health")
	logger := c.resource.logger.With(zapMethod("health"), zap.String("url", url))
	resp, err := http.Get(url)
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return rpc.NodeHealthResult_{}, err
	}

	var response rpc.NodeHealthResult_
	if err := toResponseThrift(resp, &response, logger); err != nil {
		return rpc.NodeHealthResult_{}, err
	}

	return response, nil
}

func (c *dbNode) WaitForBootstrap() error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("waitForBootstrap"))
	return c.resource.pool.Retry(func() error {
		health, err := c.Health()
		if err != nil {
			return err
		}

		if !health.GetBootstrapped() {
			err = fmt.Errorf("not bootstrapped")
			logger.Error("could not get health", zap.Error(err))
			return err
		}

		return nil
	})
}

func (c *dbNode) WritePoint(w rpc.WriteRequest) error {
	if c.resource.closed {
		return errClosed
	}

	url := c.resource.getURL(9003, "write")
	logger := c.resource.logger.With(
		zapMethod("writePoint"), zap.String("url", url))

	b, err := json.Marshal(w)
	if err != nil {
		logger.Error("could not marshal write request", zap.Error(err))
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		logger.Error("failed post", zap.Error(err))
		return err
	}

	if resp.StatusCode/100 != 2 {
		logger.Error("status code not 2xx",
			zap.Int("status code", resp.StatusCode),
			zap.String("status", resp.Status))
		return fmt.Errorf("status code %d", resp.StatusCode)
	}

	return nil
}

func (c *dbNode) Fetch(w rpc.FetchRequest) (rpc.FetchResult_, error) {
	if c.resource.closed {
		return rpc.FetchResult_{}, errClosed
	}

	url := c.resource.getURL(9003, "fetch")
	logger := c.resource.logger.With(zapMethod("fetch"), zap.String("url", url))

	b, err := json.Marshal(w)
	if err != nil {
		logger.Error("could not marshal fetch request", zap.Error(err))
		return rpc.FetchResult_{}, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		logger.Error("failed post", zap.Error(err))
		return rpc.FetchResult_{}, err
	}

	var response rpc.FetchResult_
	if err := toResponseThrift(resp, &response, logger); err != nil {
		return rpc.FetchResult_{}, err
	}

	return response, nil
}

func (c *dbNode) Restart() error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("restart"))
	logger.Info("restarting container", zap.String("conatiner", defaultDBNodeName))
	err := c.resource.pool.Client.RestartContainer(defaultDBNodeName, 60)
	if err != nil {
		logger.Error("could not restart", zap.Error(err))
		return err
	}

	return nil
}

func (c *dbNode) CheckForCheckpoint() error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("checkForCheckpoint"))
	return c.resource.pool.Retry(func() error {
		client := c.resource.pool.Client
		exec, err := client.CreateExec(docker.CreateExecOptions{
			AttachStdout: true,
			Container:    defaultDBNodeName,
			Cmd: []string{
				"find",
				"/var/lib/m3db/data/coldWritesRepairAndNoIndex",
				"-name",
				"*1-checkpoint.db"},
		})

		if err != nil {
			logger.Error("failed generating exec", zap.Error(err))
			return err
		}

		var outputBuf bytes.Buffer
		logger.Info("success", zap.String("execID", exec.ID))
		err = client.StartExec(exec.ID, docker.StartExecOptions{
			OutputStream: &outputBuf,
		})

		if err != nil {
			logger.Error("failed starting exec", zap.Error(err))
			return err
		}

		out := outputBuf.String()
		if len(out) == 0 {
			logger.Error("no output")
			return errors.New("no output")
		}

		checkpoints := strings.Split(outputBuf.String(), "\n")
		logger.Info("completed exec", zap.Strings("checkpoints", checkpoints))
		return nil
	})
}

func (c *dbNode) Close() error {
	if c.resource.closed {
		return errClosed
	}

	return c.resource.close()
}
