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

package docker

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"github.com/ory/dockertest/v3/docker/types/mount"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/integration/resources"
)

// Resource is an object that provides a handle
// to a service being spun up via docker.
type Resource struct {
	closed bool

	logger *zap.Logger

	resource *dockertest.Resource
	pool     *dockertest.Pool
}

// NewDockerResource creates a new DockerResource.
func NewDockerResource(
	pool *dockertest.Pool,
	resourceOpts ResourceOptions,
) (*Resource, error) {
	var (
		source        = resourceOpts.Source
		image         = resourceOpts.Image
		containerName = resourceOpts.ContainerName
		iOpts         = resourceOpts.InstrumentOpts
		portList      = resourceOpts.PortList

		logger = iOpts.Logger().With(
			zap.String("source", source),
			zap.String("container", containerName),
		)
	)

	opts := exposePorts(newOptions(containerName), portList)

	hostConfigOpts := func(c *dc.HostConfig) {
		c.AutoRemove = true
		c.NetworkMode = networkName
		// Allow the docker container to call services on the host machine.
		// Docker for OS X and Windows support the host.docker.internal hostname
		// natively, but Docker for Linux requires us to register host.docker.internal
		// as an extra host before the hostname works.
		if runtime.GOOS == "linux" {
			c.ExtraHosts = []string{"host.docker.internal:172.17.0.1"}
		}
		mounts := make([]dc.HostMount, 0, len(resourceOpts.TmpfsMounts))
		for _, m := range resourceOpts.TmpfsMounts {
			mounts = append(mounts, dc.HostMount{
				Target: m,
				Type:   string(mount.TypeTmpfs),
			})
		}

		c.Mounts = mounts
	}

	var resource *dockertest.Resource
	var err error
	if image.Name == "" {
		logger.Info("connecting to existing container", zap.String("container", containerName))
		var ok bool
		resource, ok = pool.ContainerByName(containerName)
		if !ok {
			logger.Error("could not find container", zap.Error(err))
			return nil, fmt.Errorf("could not find container %v", containerName)
		}
	} else {
		opts = useImage(opts, image)
		opts.Mounts = resourceOpts.Mounts
		imageWithTag := fmt.Sprintf("%v:%v", image.Name, image.Tag)
		logger.Info("running container with options",
			zap.String("image", imageWithTag), zap.Any("options", opts))
		resource, err = pool.RunWithOptions(opts, hostConfigOpts)
	}

	if err != nil {
		logger.Error("could not run container", zap.Error(err))
		return nil, err
	}

	return &Resource{
		logger:   logger,
		resource: resource,
		pool:     pool,
	}, nil
}

// GetPort retrieves the port for accessing this resource.
func (c *Resource) GetPort(bindPort int) (int, error) {
	port := c.resource.GetPort(fmt.Sprintf("%d/tcp", bindPort))
	return strconv.Atoi(port)
}

// GetURL retrieves the URL for accessing this resource.
func (c *Resource) GetURL(port int, path string) string {
	tcpPort := fmt.Sprintf("%d/tcp", port)
	return fmt.Sprintf("http://%s:%s/%s",
		c.resource.GetBoundIP(tcpPort), c.resource.GetPort(tcpPort), path)
}

// Exec runs commands within a docker container.
func (c *Resource) Exec(commands ...string) (string, error) {
	if c.closed {
		return "", errClosed
	}

	// NB: this is prefixed with a `/` that should be trimmed off.
	name := strings.TrimLeft(c.resource.Container.Name, "/")
	logger := c.logger.With(resources.ZapMethod("exec"))
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

// GoalStateExec runs commands within a container until
// a specified goal state is met.
func (c *Resource) GoalStateExec(
	verifier resources.GoalStateVerifier,
	commands ...string,
) error {
	if c.closed {
		return errClosed
	}

	logger := c.logger.With(resources.ZapMethod("GoalStateExec"))
	return c.pool.Retry(func() error {
		err := verifier(c.Exec(commands...))
		if err != nil {
			logger.Error("rerunning goal state verification", zap.Error(err))
			return err
		}

		logger.Info("goal state verification succeeded")
		return nil
	})
}

// Close closes and cleans up the resource.
func (c *Resource) Close() error {
	if c.closed {
		c.logger.Error("closing closed resource", zap.Error(errClosed))
		return errClosed
	}

	c.closed = true
	c.logger.Info("closing resource")
	return c.pool.Purge(c.resource)
}

// Closed returns true if the resource has been closed.
func (c *Resource) Closed() bool {
	return c.closed
}
