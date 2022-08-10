//// Copyright (c) 2017 Uber Technologies, Inc.
////
//// Permission is hereby granted, free of charge, to any person obtaining a copy
//// of this software and associated documentation files (the "Software"), to deal
//// in the Software without restriction, including without limitation the rights
//// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//// copies of the Software, and to permit persons to whom the Software is
//// furnished to do so, subject to the following conditions:
////
//// The above copyright notice and this permission notice shall be included in
//// all copies or substantial portions of the Software.
////
//// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
//// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
//// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
//// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
//// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
//// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
//// THE SOFTWARE.
//
package etcd

//
//import (
//	"encoding/json"
//	"fmt"
//	"io/ioutil"
//	"net/http"
//	"net/url"
//	"os"
//	"strings"
//	"time"
//
//	"github.com/m3db/m3/src/cluster/client"
//	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
//	"github.com/m3db/m3/src/cluster/services"
//	xclock "github.com/m3db/m3/src/x/clock"
//	"github.com/m3db/m3/src/x/errors"
//
//	"go.etcd.io/etcd/server/v3/embed"
//)
//
//type embeddedKV struct {
//	etcd *embed.Etcd
//	opts Options
//	dir  string
//}
//
//// New creates a new EmbeddedKV
//func New(opts Options) (EmbeddedKV, error) {
//	dir, err := ioutil.TempDir("", opts.Dir())
//	if err != nil {
//		return nil, err
//	}
//	cfg := embed.NewConfig()
//	cfg.Dir = dir
//	cfg.Logger = "zap"
//
//	setRandomPorts(cfg)
//	e, err := embed.StartEtcd(cfg)
//	if err != nil {
//		return nil, fmt.Errorf("unable to start etcd, err: %v", err)
//	}
//	return &embeddedKV{
//		etcd: e,
//		opts: opts,
//		dir:  dir,
//	}, nil
//}
//
//func setRandomPorts(cfg *embed.Config) {
//	randomPortURL, err := url.Parse("http://localhost:0")
//	if err != nil {
//		panic(err.Error())
//	}
//
//	cfg.LPUrls = []url.URL{*randomPortURL}
//	cfg.APUrls = []url.URL{*randomPortURL}
//	cfg.LCUrls = []url.URL{*randomPortURL}
//	cfg.ACUrls = []url.URL{*randomPortURL}
//
//	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
//}
//
//func (e *embeddedKV) Close() error {
//	var multi errors.MultiError
//
//	// see if there's any errors
//	select {
//	case err := <-e.etcd.Err():
//		multi = multi.Add(err)
//	default:
//	}
//
//	// shutdown and release
//	e.etcd.Server.Stop()
//	e.etcd.Close()
//
//	multi = multi.Add(os.RemoveAll(e.dir))
//	return multi.FinalError()
//}
//
//func (e *embeddedKV) Start() error {
//	timeout := e.opts.InitTimeout()
//	select {
//	case <-e.etcd.Server.ReadyNotify():
//		break
//	case <-time.After(timeout):
//		return fmt.Errorf("etcd server took too long to start")
//	}
//
//	// ensure v3 api endpoints are available, https://github.com/coreos/etcd/pull/7075
//	apiVersionEndpoint := fmt.Sprintf("http://%s/version", e.etcd.Clients[0].Addr().String())
//	fn := func() bool { return version3Available(apiVersionEndpoint) }
//	ok := xclock.WaitUntil(fn, timeout)
//	if !ok {
//		return fmt.Errorf("api version 3 not available")
//	}
//
//	return nil
//}
//
//type versionResponse struct {
//	Version string `json:"etcdcluster"`
//}
//
//func version3Available(endpoint string) bool {
//	resp, err := http.Get(endpoint)
//	if err != nil {
//		return false
//	}
//	if resp.StatusCode != 200 {
//		return false
//	}
//	defer resp.Body.Close()
//
//	decoder := json.NewDecoder(resp.Body)
//	var data versionResponse
//	err = decoder.Decode(&data)
//	if err != nil {
//		return false
//	}
//
//	return strings.Index(data.Version, "3.") == 0
//}
//
//func (e *embeddedKV) Endpoints() []string {
//	addresses := make([]string, 0, len(e.etcd.Clients))
//	for _, c := range e.etcd.Clients {
//		addresses = append(addresses, c.Addr().String())
//	}
//	return addresses
//}
//
//func (e *embeddedKV) ConfigServiceClient(fns ...ClientOptFn) (client.Client, error) {
//	eopts := etcdclient.NewOptions().
//		SetInstrumentOptions(e.opts.InstrumentOptions()).
//		SetServicesOptions(services.NewOptions().SetInitTimeout(e.opts.InitTimeout())).
//		SetClusters([]etcdclient.Cluster{
//			etcdclient.NewCluster().SetZone(e.opts.Zone()).SetEndpoints(e.Endpoints()),
//		}).
//		SetService(e.opts.ServiceID()).
//		SetEnv(e.opts.Environment()).
//		SetZone(e.opts.Zone())
//	for _, fn := range fns {
//		eopts = fn(eopts)
//	}
//	return etcdclient.NewConfigServiceClient(eopts)
//}
