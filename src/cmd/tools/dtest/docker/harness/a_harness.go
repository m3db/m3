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
	"time"

	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/instrument"
	dockertest "github.com/ory/dockertest"
	"go.uber.org/zap"
)

func setupColdWrites() error {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return err
	}

	pool.MaxWait = time.Second * 10
	err = setupNetwork(pool)
	if err != nil {
		return err
	}

	err = setupVolume(pool)
	if err != nil {
		return err
	}

	iOpts := instrument.NewOptions()
	dbNode, err := newDockerHTTPNode(pool, dockerResourceOptions{
		iOpts: iOpts,
	})

	if err != nil {
		return err
	}

	dbNodes := []Node{dbNode}
	defer func() {
		dbNode.Close()
	}()

	coordinator, err := newDockerHTTPCoordinator(pool, dockerResourceOptions{
		iOpts: iOpts,
	})

	if err != nil {
		return err
	}

	defer func() {
		coordinator.Close()
	}()

	logger := iOpts.Logger().With(zap.String("source", "harness"))
	hosts := make([]*admin.Host, 0, len(dbNodes))
	ids := make([]string, 0, len(dbNodes))
	for _, n := range dbNodes {
		h, err := n.HostDetails()
		if err != nil {
			logger.Error("could not get host details", zap.Error(err))
			return err
		}

		hosts = append(hosts, h)
		ids = append(ids, h.GetId())
	}

	var (
		aggName   = "aggregated"
		unaggName = "unaggregated"
		retention = "6h"
	)

	aggDatabase := admin.DatabaseCreateRequest{
		Type:              "cluster",
		NamespaceName:     aggName,
		RetentionTime:     retention,
		NumShards:         4,
		ReplicationFactor: 1,
		Hosts:             hosts,
	}

	unaggDatabase := admin.DatabaseCreateRequest{
		NamespaceName: unaggName,
		RetentionTime: retention,
	}

	logger.Info("waiting for coordinator")
	if err := coordinator.WaitForNamespace(""); err != nil {
		return err
	}

	logger.Info("creating database", zap.Any("request", aggDatabase))
	if _, err := coordinator.CreateDatabase(aggDatabase); err != nil {
		return err
	}

	logger.Info("waiting for placements", zap.Strings("placement ids", ids))
	if err := coordinator.WaitForPlacements(ids); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", aggName))
	if err := coordinator.WaitForNamespace(aggName); err != nil {
		return err
	}

	logger.Info("creating namespace", zap.Any("request", unaggDatabase))
	if _, err := coordinator.CreateDatabase(unaggDatabase); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", unaggName))
	if err := coordinator.WaitForNamespace(unaggName); err != nil {
		return err
	}

	return err
}
