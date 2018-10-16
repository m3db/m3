// Copyright (c) 2018 Uber Technologies, Inc.
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

package config

import (
	"github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/metrics/matcher"
	"github.com/m3db/m3/src/metrics/matcher/cache"
	etcdclient "github.com/m3db/m3cluster/client/etcd"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/config/listenaddress"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/pool"

	"go.uber.org/zap"
)

// Configuration is configuration for the collector.
type Configuration struct {
	Metrics       instrument.MetricsConfiguration `yaml:"metrics"`
	Logging       zap.Config                      `yaml:"logging"`
	ListenAddress listenaddress.Configuration     `yaml:"listenAddress" validate:"nonzero"`
	Etcd          etcdclient.Configuration        `yaml:"etcd"`
	Reporter      ReporterConfiguration           `yaml:"reporter"`
}

// ReporterConfiguration is the collector
type ReporterConfiguration struct {
	Cache                 cache.Configuration          `yaml:"cache" validate:"nonzero"`
	Matcher               matcher.Configuration        `yaml:"matcher" validate:"nonzero"`
	Client                client.Configuration         `yaml:"client"`
	SortedTagIteratorPool pool.ObjectPoolConfiguration `yaml:"sortedTagIteratorPool"`
	Clock                 clock.Configuration          `yaml:"clock"`
}
