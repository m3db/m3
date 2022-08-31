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
	"errors"
	"fmt"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
)

var (
	networkName = "d-test"
	volumeName  = "d-test"

	// ErrClosed is a common error for use when a container has been closed.
	ErrClosed = errors.New("container has been closed")
)

// Image represents a docker image.
type Image struct {
	Name string
	Tag  string
}

// GoalStateVerifier asserts that a resource is in a particular state.
// TODO: more info here; this interface is unclear from usage
type GoalStateVerifier func(output string, err error) error

// ResourceOptions returns options for creating
// a Resource.
//nolint:maligned
type ResourceOptions struct {
	OverrideDefaults bool
	Source           string
	ContainerName    string
	Image            Image
	PortList         []int
	PortMappings     map[dc.Port][]dc.PortBinding
	NoNetworkOverlay bool

	// Env is the environment for the docker container; it corresponds 1:1 with dockertest.RunOptions.
	// Format should be: VAR=value
	Env []string
	// Mounts creates mounts in the container that map back to a resource
	// on the host system.
	Mounts []string
	// TmpfsMounts creates mounts to the container's temporary file system
	TmpfsMounts    []string
	InstrumentOpts instrument.Options
}

// NB: this will fill unset fields with given default values.
func (o ResourceOptions) WithDefaults(
	defaultOpts ResourceOptions) ResourceOptions {
	if o.OverrideDefaults {
		return o
	}

	if len(o.Source) == 0 {
		o.Source = defaultOpts.Source
	}

	if len(o.ContainerName) == 0 {
		o.ContainerName = defaultOpts.ContainerName
	}

	if o.Image == (Image{}) {
		o.Image = defaultOpts.Image
	}

	if len(o.PortList) == 0 {
		o.PortList = defaultOpts.PortList
	}

	if len(o.TmpfsMounts) == 0 {
		o.TmpfsMounts = defaultOpts.TmpfsMounts
	}

	if len(o.Mounts) == 0 {
		o.Mounts = defaultOpts.Mounts
	}

	if o.InstrumentOpts == nil {
		o.InstrumentOpts = defaultOpts.InstrumentOpts
	}

	return o
}

func newOptions(name string) *dockertest.RunOptions {
	return &dockertest.RunOptions{
		Name:      name,
		NetworkID: networkName,
	}
}

func useImage(opts *dockertest.RunOptions, image Image) *dockertest.RunOptions {
	opts.Repository = image.Name
	opts.Tag = image.Tag
	return opts
}

// SetupNetwork sets up a network within docker.
func SetupNetwork(pool *dockertest.Pool, cleanIfExists bool) error {
	networks, err := pool.Client.ListNetworks()
	if err != nil {
		return err
	}

	for _, n := range networks {
		if n.Name == networkName {
			if !cleanIfExists {
				return nil
			}
			if err := pool.Client.RemoveNetwork(networkName); err != nil {
				return err
			}

			break
		}
	}

	_, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: networkName})
	return err
}

// SetupVolume creates a default docker volume, with name volumeName (in this package)
func SetupVolume(pool *dockertest.Pool) error {
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
	mappings map[dc.Port][]dc.PortBinding,
) (*dockertest.RunOptions, error) {
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

	for k, v := range mappings {
		if _, ok := ports[k]; ok {
			return nil, fmt.Errorf("mapping %s already specified by PortList; "+
				"mappings should be in PortList or PortMappings but not both",
				k,
			)
		}
		ports[k] = v
	}

	opts.PortBindings = ports
	return opts, nil
}
