// Copyright (c) 2017 Uber Technologies, Inc.
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

package etcd

import (
	"time"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3x/instrument"
)

// M3SDConfiguration is the config for service discovery
type M3SDConfiguration struct {
	InitTimeout time.Duration `yaml:"initTimeout"`
}

// ClusterConfig is the config for a zoned etcd cluster
type ClusterConfig struct {
	Zone      string   `yaml:"zone"`
	Endpoints []string `yaml:"endpoints"`
}

// Configuration is for config service client
type Configuration struct {
	Zone         string            `yaml:"zone"`
	Env          string            `yaml:"env"`
	Service      string            `yaml:"service" validate:"nonzero"`
	CacheDir     string            `yaml:"cacheDir"`
	ETCDClusters []ClusterConfig   `yaml:"etcdClusters"`
	M3SD         M3SDConfiguration `yaml:"m3sd"`
}

// NewClient creates a new config service client
func (cfg Configuration) NewClient(iopts instrument.Options) (client.Client, error) {
	return NewConfigServiceClient(cfg.NewOptions().SetInstrumentOptions(iopts))
}

// NewOptions retuns a new Options
func (cfg Configuration) NewOptions() Options {
	return NewOptions().
		SetZone(cfg.Zone).
		SetEnv(cfg.Env).
		SetService(cfg.Service).
		SetCacheDir(cfg.CacheDir).
		SetServiceInitTimeout(cfg.M3SD.InitTimeout).
		SetClusters(cfg.etcdClusters())
}

func (cfg Configuration) etcdClusters() []Cluster {
	res := make([]Cluster, len(cfg.ETCDClusters))
	for i, c := range cfg.ETCDClusters {
		res[i] = NewCluster().
			SetZone(c.Zone).
			SetEndpoints(c.Endpoints)
	}

	return res
}
