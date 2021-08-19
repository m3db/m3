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

package resources

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/ory/dockertest/v3/docker/types/mount"
	"go.uber.org/zap"
)

type dockerResource struct {
	closed bool

	logger *zap.Logger

	resource *dockertest.Resource
	pool     *dockertest.Pool
}

func newDockerResource(
	pool *dockertest.Pool,
	resourceOpts dockerResourceOptions,
) (*dockerResource, error) {
	var (
		source        = resourceOpts.source
		image         = resourceOpts.image
		containerName = resourceOpts.containerName
		iOpts         = resourceOpts.iOpts
		portList      = resourceOpts.portList

		logger = iOpts.Logger().With(
			zap.String("source", source),
			zap.String("container", containerName),
		)
	)

	opts := exposePorts(newOptions(containerName), portList)

	hostConfigOpts := func(c *dc.HostConfig) {
		c.NetworkMode = networkName
		mounts := make([]dc.HostMount, 0, len(resourceOpts.mounts))
		for _, m := range resourceOpts.mounts {
			mounts = append(mounts, dc.HostMount{
				Target: m,
				Type:   string(mount.TypeTmpfs),
			})
		}

		c.Mounts = mounts
	}

	var resource *dockertest.Resource
	var err error
	if image.name == "" {
		logger.Info("connecting to existing container", zap.String("container", containerName))
		var ok bool
		resource, ok = pool.ContainerByName(containerName)
		if !ok {
			logger.Error("could not find container", zap.Error(err))
			return nil, fmt.Errorf("could not find container %v", containerName)
		}
	} else {
		opts = useImage(opts, image)
		imageWithTag := fmt.Sprintf("%v:%v", image.name, image.tag)
		logger.Info("running container with options",
			zap.String("image", imageWithTag), zap.Any("options", opts))
		resource, err = pool.RunWithOptions(opts, hostConfigOpts)
	}

	if err != nil {
		logger.Error("could not run container", zap.Error(err))
		return nil, err
	}

	return &dockerResource{
		logger:   logger,
		resource: resource,
		pool:     pool,
	}, nil
}

func (c *dockerResource) getPort(bindPort int) (int, error) {
	port := c.resource.GetPort(fmt.Sprintf("%d/tcp", bindPort))
	return strconv.Atoi(port)
}

func (c *dockerResource) getURL(port int, path string) string {
	tcpPort := fmt.Sprintf("%d/tcp", port)
	return fmt.Sprintf("http://%s:%s/%s",
		c.resource.GetBoundIP(tcpPort), c.resource.GetPort(tcpPort), path)
}

func (c *dockerResource) exec(commands ...string) (string, error) {
	if c.closed {
		return "", errClosed
	}

	// NB: this is prefixed with a `/` that should be trimmed off.
	name := strings.TrimLeft(c.resource.Container.Name, "/")
	logger := c.logger.With(zapMethod("exec"))
	client := c.pool.Client
	exec, err := client.CreateExec(dc.CreateExecOptions{
		AttachStdout: true,
		AttachStderr: true,
		Container:    name,
		Cmd:          commands,
	})

	if err != nil {
		logger.Error("failed generating exec", zap.Error(err))
		return "", err
	}

	var outBuf, errBuf bytes.Buffer
	logger.Info("starting exec",
		zap.Strings("commands", commands),
		zap.String("execID", exec.ID))
	err = client.StartExec(exec.ID, dc.StartExecOptions{
		OutputStream: &outBuf,
		ErrorStream:  &errBuf,
	})

	output, bufferErr := outBuf.String(), errBuf.String()
	logger = logger.With(zap.String("stdout", output),
		zap.String("stderr", bufferErr))

	if err != nil {
		logger.Error("failed starting exec",
			zap.Error(err))
		return "", err
	}

	if len(bufferErr) != 0 {
		err = errors.New(bufferErr)
		logger.Error("exec failed", zap.Error(err))
		return "", err
	}

	logger.Info("succeeded exec")
	return output, nil
}

func (c *dockerResource) goalStateExec(
	verifier GoalStateVerifier,
	commands ...string,
) error {
	if c.closed {
		return errClosed
	}

	logger := c.logger.With(zapMethod("goalStateExec"))
	return c.pool.Retry(func() error {
		err := verifier(c.exec(commands...))
		if err != nil {
			logger.Error("rerunning goal state verification", zap.Error(err))
			return err
		}

		logger.Info("goal state verification succeeded")
		return nil
	})
}

func (c *dockerResource) close() error {
	if c.closed {
		c.logger.Error("closing closed resource", zap.Error(errClosed))
		return errClosed
	}

	c.closed = true
	c.logger.Info("closing resource")
	return c.pool.Purge(c.resource)
}
