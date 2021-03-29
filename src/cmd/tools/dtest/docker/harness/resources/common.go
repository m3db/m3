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
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3/src/x/instrument"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	networkName = "d-test"
	volumeName  = "d-test"

	errClosed = errors.New("container has been closed")
)

func zapMethod(s string) zapcore.Field { return zap.String("method", s) }

type dockerResourceOptions struct {
	overrideDefaults bool
	source           string
	containerName    string
	image            dockerImage
	portList         []int
	mounts           []string
	iOpts            instrument.Options
}

// NB: this will fill unset fields with given default values.
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

	if o.image == (dockerImage{}) {
		o.image = defaultOpts.image
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

func newOptions(name string) *dockertest.RunOptions {
	return &dockertest.RunOptions{
		Name:      name,
		NetworkID: networkName,
	}
}

func useImage(opts *dockertest.RunOptions, image dockerImage) *dockertest.RunOptions {
	opts.Repository = image.name
	opts.Tag = image.tag
	return opts
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

func toResponse(
	resp *http.Response,
	response proto.Message,
	logger *zap.Logger,
) error {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("could not read body", zap.Error(err))
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		logger.Error("status code not 2xx",
			zap.Int("status code", resp.StatusCode),
			zap.String("status", resp.Status))
		return fmt.Errorf("status code %d", resp.StatusCode)
	}

	err = jsonpb.Unmarshal(bytes.NewReader(b), response)
	if err != nil {
		logger.Error("unable to unmarshal response",
			zap.Error(err),
			zap.Any("response", response))
		return err
	}

	return nil
}
