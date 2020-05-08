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
	"net/http"
	"testing"
	"time"

	dockertest "github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	nodeDockerfile = "./m3dbnode.Dockerfile"
	nodeName       = "dbnode01"

	networkName = "d-test"
	volume      = "/etc/m3coordinator/"
)

var (
	volumeName = fmt.Sprintf(
		"/Users/arnikola/go/src/github.com/m3db/m3/scripts/"+
			"docker-integration-tests/cold_writes_simple:%s", volume)
)

func newOptions(name string) *dockertest.RunOptions {
	return &dockertest.RunOptions{
		Name:      name,
		NetworkID: networkName,
	}
}

func exposePorts(opts *dockertest.RunOptions, portList ...int) *dockertest.RunOptions {
	// expose := make([]string, 0, len(portList))
	ports := make(map[dc.Port][]dc.PortBinding, len(portList))
	for _, p := range portList {
		port := fmt.Sprintf("%d", p)
		// expose = append(expose, port)

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

	// opts.ExposedPorts = expose
	opts.PortBindings = ports
	return opts
}

func buildNode(t *testing.T, pool *dockertest.Pool) *dockertest.Resource {
	require.NoError(t, pool.RemoveContainerByName(nodeName))
	opts := exposePorts(newOptions(nodeName), 2379, 2380, 9000, 9001, 9002, 9003, 9004)
	dbNode, err := pool.BuildAndRunWithOptions(nodeDockerfile, opts,
		func(c *dc.HostConfig) {
			c.NetworkMode = networkName
		})
	require.NoError(t, err)
	return dbNode
}

func setupNetwork(t *testing.T, pool *dockertest.Pool) {
	networks, err := pool.Client.ListNetworks()
	require.NoError(t, err)
	for _, n := range networks {
		if n.Name == networkName {
			require.NoError(t, pool.Client.RemoveNetwork(networkName))
			break
		}
	}

	_, err = pool.Client.CreateNetwork(dc.CreateNetworkOptions{Name: networkName})
	require.NoError(t, err)
}

func setupVolume(t *testing.T, pool *dockertest.Pool) {
	volumes, err := pool.Client.ListVolumes(dc.ListVolumesOptions{})
	require.NoError(t, err)
	for _, v := range volumes {
		if volume == v.Name {
			require.NoError(t, pool.Client.RemoveVolume(volume))
		}

	}

	pool.Client.CreateVolume(dc.CreateVolumeOptions{
		Name: volumeName,
	})
}

func TestDockerSetup(t *testing.T) {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Second * 20

	setupNetwork(t, pool)
	setupVolume(t, pool)

	dbnode := buildNode(t, pool)
	defer func() {
		assert.NoError(t, pool.Purge(dbnode))
	}()

	coordinator := buildCoordinator(t, pool)
	defer func() {
		assert.NoError(t, pool.Purge(coordinator))
	}()

	cPort := coordinator.GetPort("7201/tcp")
	coordURL := fmt.Sprintf("http://0.0.0.0:%s/api/v1/namespace", cPort)
	fmt.Println(cPort)

	fmt.Println("db 2379", dbnode.GetPort("2379/tcp"))
	fmt.Println("db port", dbnode.GetPort("9000/tcp"))
	fmt.Println("db health port", dbnode.GetPort("9002/tcp"))

	err = pool.Retry(func() error {
		resp, err := http.Get(coordURL)
		if err != nil {
			fmt.Println("err", err)
			return err
		}

		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("status code %d", resp.StatusCode)
		}

		return nil
	})

	assert.NoError(t, err)
}
