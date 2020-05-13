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
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/m3db/m3/src/x/instrument"
	dockertest "github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"go.uber.org/zap"
)

var (
	networkName = "d-test"
	volumeName  = "d-test"

	errClosed = errors.New("coordinator container has been closed")
)

type dockerResourceOptions struct {
	overrideDefaults bool
	source           string
	containerName    string
	dockerFile       string
	defaultPort      string
	portList         []int
	mounts           []string
	iOpts            instrument.Options
}

// Fill unset fields with default values.
func (o dockerResourceOptions) withDefaults(
	defaultOpts dockerResourceOptions) dockerResourceOptions {
	if o.overrideDefaults {
		return o
	}

	if len(o.source) == 0 {
		o.source = defaultOpts.source
	}

	if len(o.containerName) == 0 {
		o.containerName = defaultOpts.containerName
	}

	if len(o.dockerFile) == 0 {
		o.dockerFile = defaultOpts.dockerFile
	}

	if len(o.defaultPort) == 0 {
		o.defaultPort = defaultOpts.defaultPort
	}

	if len(o.portList) == 0 {
		o.portList = defaultOpts.portList
	}

	if len(o.mounts) == 0 {
		o.mounts = defaultOpts.mounts
	}

	if o.iOpts == nil {
		o.iOpts = defaultOpts.iOpts
	}

	return o
}

type dockerResource struct {
	baseURL string
	closed  bool

	logger *zap.Logger

	resource *dockertest.Resource
	pool     *dockertest.Pool
}

func newOptions(name string) *dockertest.RunOptions {
	return &dockertest.RunOptions{
		Name:      name,
		NetworkID: networkName,
	}
}

func setupNetwork(pool *dockertest.Pool) error {
	networks, err := pool.Client.ListNetworks()
	if err != nil {
		return err
	}

	for _, n := range networks {
		if n.Name == networkName {
			if err := pool.Client.RemoveNetwork(networkName); err != nil {
				return err
			}

			break
		}
	}

	_, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: networkName})
	return err
}

func setupVolume(pool *dockertest.Pool) error {
	volumes, err := pool.Client.ListVolumes(dc.ListVolumesOptions{})
	if err != nil {
		return err
	}

	for _, v := range volumes {
		if volumeName == v.Name {
			if err := pool.Client.RemoveVolume(volumeName); err != nil {
				return err
			}

			break
		}
	}

	_, err = pool.Client.CreateVolume(dc.CreateVolumeOptions{
		Name: volumeName,
	})

	return err
}

func exposePorts(
	opts *dockertest.RunOptions,
	portList []int,
) *dockertest.RunOptions {
	ports := make(map[dc.Port][]dc.PortBinding, len(portList))
	for _, p := range portList {
		port := fmt.Sprintf("%d", p)

		portRepresentation := dc.Port(fmt.Sprintf("%s/tcp", port))
		binding := dc.PortBinding{HostIP: "0.0.0.0", HostPort: port}
		entry, found := ports[portRepresentation]
		if !found {
			entry = []dc.PortBinding{binding}
		} else {
			entry = append(entry, binding)
		}

		ports[portRepresentation] = entry
	}

	opts.PortBindings = ports
	return opts
}

func setupMount(dest string) string {
	src := os.TempDir()
	src = "/Users/arnikola/go/src/github.com/m3db/m3/scripts/" +
		"docker-integration-tests/cold_writes_simple"
	return fmt.Sprintf("%s:%s", src, dest)
}

func newDockerResource(
	pool *dockertest.Pool,
	resourceOpts dockerResourceOptions,
) (*dockerResource, error) {
	var (
		source        = resourceOpts.source
		containerName = resourceOpts.containerName
		dockerFile    = resourceOpts.dockerFile
		iOpts         = resourceOpts.iOpts
		defaultPort   = resourceOpts.defaultPort
		portList      = resourceOpts.portList

		logger = iOpts.Logger().With(
			zap.String("source", source),
			zap.String("container name", containerName),
		)
	)

	if err := pool.RemoveContainerByName(containerName); err != nil {
		logger.Error("could not remove container from pool", zap.Error(err))
		return nil, err
	}

	opts := exposePorts(newOptions(containerName), portList)
	opts.Mounts = resourceOpts.mounts

	logger.Info("building container with options", zap.Any("options", opts))
	resource, err := pool.BuildAndRunWithOptions(dockerFile, opts,
		func(c *dc.HostConfig) {
			c.NetworkMode = networkName
		})

	if err != nil {
		logger.Error("could not build and run container", zap.Error(err))
		return nil, err
	}

	url := fmt.Sprintf("http://%s:%s",
		resource.GetBoundIP(defaultPort), resource.GetPort(defaultPort))

	// NB: 8 == len("http://:"")
	if len(url) <= 8 {
		err := errors.New("could not get host port for resource")
		if purgeErr := pool.Purge(resource); purgeErr != nil {
			logger.Error("could not tear down failed resource",
				zap.Error(purgeErr), zap.String("base error", err.Error()))
			return nil, purgeErr
		}

		logger.Error("no host port for port", zap.String("port", defaultPort))
		return nil, errors.New("could not get host port")
	}

	return &dockerResource{
		baseURL:  url,
		logger:   logger.With(zap.String("base", url)),
		resource: resource,
		pool:     pool,
	}, nil
}

func (c *dockerResource) getPort(bindPort int) (int, error) {
	port := c.resource.GetPort(fmt.Sprintf("%d/tcp", bindPort))
	return strconv.Atoi(port)
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
