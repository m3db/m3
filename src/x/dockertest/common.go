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
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/ory/dockertest/v4"

	"github.com/m3db/m3/src/x/instrument"
)

var (
	networkName = "d-test"

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
//
//nolint:maligned
type ResourceOptions struct {
	OverrideDefaults bool
	Source           string
	ContainerName    string
	Image            Image
	PortList         []int
	PortMappings     network.PortMap

	// NoNetworkOverlay if set, disables use of the default integration testing network we create (networkName).
	NoNetworkOverlay bool

	Cmd []string

	// Env is the environment for the docker container; it is passed through to dockertest.WithEnv.
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

// SetupNetwork sets up a network within docker.
func SetupNetwork(ctx context.Context, pool dockertest.Pool, cleanIfExists bool) error {
	client := pool.Client()
	networks, err := client.NetworkList(ctx, mobyclient.NetworkListOptions{})
	if err != nil {
		return err
	}

	for _, n := range networks.Items {
		if n.Name == networkName {
			if !cleanIfExists {
				return nil
			}
			if _, err := client.NetworkRemove(ctx, networkName, mobyclient.NetworkRemoveOptions{}); err != nil {
				return err
			}

			break
		}
	}

	_, err = client.NetworkCreate(ctx, networkName, mobyclient.NetworkCreateOptions{})
	return err
}

// TCPPort returns the moby representation of a TCP container port, e.g. "9000/tcp".
func TCPPort(port int) network.Port {
	return network.MustParsePort(fmt.Sprintf("%d/tcp", port))
}

func exposePorts(
	portList []int,
	mappings network.PortMap,
) (network.PortMap, error) {
	ports := make(network.PortMap, len(portList)+len(mappings))
	for _, p := range portList {
		port := fmt.Sprintf("%d", p)
		portRepresentation, err := network.ParsePort(fmt.Sprintf("%s/tcp", port))
		if err != nil {
			return nil, err
		}
		binding := network.PortBinding{HostIP: netip.IPv4Unspecified(), HostPort: port}
		ports[portRepresentation] = append(ports[portRepresentation], binding)
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

	return ports, nil
}
