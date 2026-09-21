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

package dockertest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"

	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobyclient "github.com/moby/moby/client"
	"github.com/ory/dockertest/v4"
	"go.uber.org/zap"
)

// Resource is an object that provides a handle
// to a service being spun up via docker.
type Resource struct {
	resource dockertest.Resource
	// closable is set only for containers started by this package. Containers
	// attached to by name (see NewDockerResource) are not tracked by the pool
	// and are removed directly through the docker client on Close.
	closable dockertest.ClosableResource
	closed   bool

	logger *zap.Logger

	pool dockertest.Pool
}

// NewDockerResource creates a new DockerResource.
// If resourceOpts.Image is empty, it will attempt to connect to an existing container.
// Otherwise, it will start the container with the specified image.
func NewDockerResource(
	ctx context.Context,
	pool dockertest.Pool,
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

	// TODO: this seems hard to use; a different method might be more appropriate.
	if image.Name == "" {
		logger.Info("connecting to existing container", zap.String("container", containerName))
		inspected, err := pool.Client().ContainerInspect(ctx, containerName, mobyclient.ContainerInspectOptions{})
		if err != nil {
			logger.Error("could not find container", zap.Error(err))
			return nil, fmt.Errorf("could not find container %v: %w", containerName, err)
		}

		return &Resource{
			logger:   logger,
			resource: dockertest.NewResource(inspected.Container),
			pool:     pool,
		}, nil
	}

	ports, err := exposePorts(portList, resourceOpts.PortMappings)
	if err != nil {
		return nil, err
	}

	hostConfigOpts := func(c *container.HostConfig) {
		if !resourceOpts.NoNetworkOverlay {
			c.NetworkMode = container.NetworkMode(networkName)
		}
		// Allow the docker container to call services on the host machine.
		// Docker for OS X and Windows support the host.docker.internal hostname
		// natively, but Docker for Linux requires us to register host.docker.internal
		// as an extra host before the hostname works.
		if runtime.GOOS == "linux" {
			c.ExtraHosts = []string{"host.docker.internal:172.17.0.1"}
		}
		mounts := make([]mount.Mount, 0, len(resourceOpts.TmpfsMounts))
		for _, m := range resourceOpts.TmpfsMounts {
			mounts = append(mounts, mount.Mount{
				Target: m,
				Type:   mount.TypeTmpfs,
			})
		}

		c.Mounts = mounts
	}

	runOpts := []dockertest.RunOption{
		// dockertest v4 reuses containers keyed on repository:tag by default,
		// which would collapse independently named nodes (e.g. multi-node etcd or
		// dbnode clusters) into a single shared container. Every resource created
		// here is named uniquely by its caller, so always run a fresh container to
		// preserve the v3 semantics.
		dockertest.WithoutReuse(),
		dockertest.WithName(containerName),
		dockertest.WithPortBindings(ports),
		dockertest.WithHostConfig(hostConfigOpts),
		dockertest.WithMounts(resourceOpts.Mounts),
		dockertest.WithEnv(resourceOpts.Env),
		dockertest.WithCmd(resourceOpts.Cmd),
	}
	if image.Tag != "" {
		runOpts = append(runOpts, dockertest.WithTag(image.Tag))
	}

	imageWithTag := fmt.Sprintf("%v:%v", image.Name, image.Tag)
	logger.Info("running container with options",
		zap.String("image", imageWithTag),
		zap.Strings("cmd", resourceOpts.Cmd),
		zap.Strings("env", resourceOpts.Env),
		zap.Strings("mounts", resourceOpts.Mounts),
		zap.Strings("tmpfsMounts", resourceOpts.TmpfsMounts),
		zap.Any("ports", ports),
		zap.Bool("noNetworkOverlay", resourceOpts.NoNetworkOverlay))
	resource, err := pool.Run(ctx, image.Name, runOpts...)
	if err != nil {
		logger.Error("could not run container", zap.Error(err))
		return nil, err
	}

	return &Resource{
		logger:   logger,
		resource: resource,
		closable: resource,
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
	hostPort := net.JoinHostPort(c.resource.GetBoundIP(tcpPort), c.resource.GetPort(tcpPort))
	return fmt.Sprintf("http://%s/%s", hostPort, path)
}

// Exec runs commands within a docker container.
func (c *Resource) Exec(commands ...string) (string, error) {
	if c.closed {
		return "", ErrClosed
	}

	ctx := context.Background()

	// NB: this is prefixed with a `/` that should be trimmed off.
	name := strings.TrimLeft(c.resource.Container().Name, "/")
	logger := c.logger.With(zap.String("method", "exec"))
	client := c.pool.Client()
	exec, err := client.ExecCreate(ctx, name, mobyclient.ExecCreateOptions{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          commands,
	})
	if err != nil {
		logger.Error("failed generating exec", zap.Error(err))
		return "", err
	}

	logger.Info("starting exec",
		zap.Strings("commands", commands),
		zap.String("execID", exec.ID))
	attached, err := client.ExecAttach(ctx, exec.ID, mobyclient.ExecAttachOptions{})
	if err != nil {
		logger.Error("failed starting exec", zap.Error(err))
		return "", err
	}
	defer attached.Conn.Close() //nolint:errcheck

	var outBuf, errBuf bytes.Buffer
	_, err = stdcopy.StdCopy(&outBuf, &errBuf, attached.Reader)

	output, bufferErr := outBuf.String(), errBuf.String()
	logger = logger.With(zap.String("stdout", output),
		zap.String("stderr", bufferErr))

	if err != nil {
		logger.Error("failed reading exec output",
			zap.Error(err))
		return "", err
	}

	// NB: as in the dockertest v3 implementation, any output on stderr is
	// treated as failure regardless of the exit code.
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
	verifier GoalStateVerifier,
	commands ...string,
) error {
	if c.closed {
		return ErrClosed
	}

	logger := c.logger.With(zap.String("method", "GoalStateExec"))
	// NB: a zero timeout uses the pool's configured max wait.
	return c.pool.Retry(context.Background(), 0, func() error {
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
		c.logger.Error("closing closed resource", zap.Error(ErrClosed))
		return ErrClosed
	}

	c.closed = true
	c.logger.Info("closing resource")

	ctx := context.Background()
	if c.closable != nil {
		return c.closable.Close(ctx)
	}

	// Attached containers are not tracked by the pool; remove them directly,
	// matching what dockertest v3's Purge did.
	_, err := c.pool.Client().ContainerRemove(ctx, c.resource.Container().ID, mobyclient.ContainerRemoveOptions{
		Force:         true,
		RemoveVolumes: true,
	})
	return err
}

// Closed returns true if the resource has been closed.
func (c *Resource) Closed() bool {
	return c.closed
}

// Resource is the underlying dockertest resource used by this Resource. It can be used to perform more advanced
// operations not exposed by this class.
func (c *Resource) Resource() dockertest.Resource {
	return c.resource
}
