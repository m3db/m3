// Copyright (c) 2023 Uber Technologies, Inc.
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

package client

import (
	"github.com/m3db/m3/src/dbnode/auth"
	"github.com/m3db/m3/src/dbnode/environment"
)

// PopulateClientOutboundAuthConfig takes clients dynamic config as populate auth global registry.
func PopulateClientOutboundAuthConfig(cfg environment.DynamicConfiguration) error {
	var dbnodeCredentials []auth.OutboundCredentials
	var etcdCredentails []auth.OutboundCredentials

	for _, clusterCfg := range cfg {
		if ok := clusterCfg.Service.Auth.Enabled; ok {
			if err := clusterCfg.Service.Auth.Validate(); err != nil {
				return err
			}
			nodeCredential := auth.OutboundCredentials{
				Username: clusterCfg.Service.Auth.UserName,
				Password: clusterCfg.Service.Auth.Password,
				Zone:     clusterCfg.Service.Zone,
				Type:     auth.ClientCredential,
			}
			dbnodeCredentials = append(dbnodeCredentials, nodeCredential)
		}

		etcdClusters := clusterCfg.Service.ETCDClusters
		for _, etcdCluster := range etcdClusters {
			if ok := etcdCluster.Auth.Enabled; ok {
				if err := etcdCluster.Auth.Validate(); err != nil {
					return err
				}
				etcdCredential := auth.OutboundCredentials{
					Username: etcdCluster.Auth.UserName,
					Password: etcdCluster.Auth.Password,
					Zone:     etcdCluster.Zone,
					Type:     auth.EtcdCredential,
				}
				etcdCredentails = append(etcdCredentails, etcdCredential)
			}
		}
	}
	auth.PopulateOutbound(dbnodeCredentials, etcdCredentails)
	return nil
}
