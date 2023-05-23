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

package server

import (
	"strings"

	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/auth"
)

var (
	authModeMap = map[string]auth.Mode{
		"none":    auth.AuthModeNoAuth,
		"shadow":  auth.AuthModeShadow,
		"enabled": auth.AuthModeEnforced,
	}
)

// PopulateInboundAuthConfig populates inbound auth modules with the provided auth config.
func PopulateInboundAuthConfig(cfg config.AuthConfig) {
	nodeInbound := cfg.Inbound.M3DB
	inboundAuth := make([]auth.InboundCredentials, 0, len(nodeInbound.Credentials))
	for _, nodeCfg := range nodeInbound.Credentials {
		inboundAuth = append(inboundAuth, auth.InboundCredentials{
			Username: *nodeCfg.Username,
			Digest:   *nodeCfg.Digest,
			Type:     auth.ClientCredential,
		})
	}
	authMode := parseAuthMode(*cfg.Inbound.M3DB.Mode)
	auth.PopulateInbound(inboundAuth, authMode)
}

// RefreshInboundAuthConfig take AuthConfig as input param and populates inbound credentials global module.
func RefreshInboundAuthConfig(credentialsConfig config.AuthConfig) {
	auth.InboundLock.Lock()
	defer auth.InboundLock.Unlock()
	PopulateInboundAuthConfig(credentialsConfig)
}

// PopulateOutboundAuthConfig populates outbound auth modules with the provided auth config.
func PopulateOutboundAuthConfig(cfg config.AuthConfig) {
	nodeOutboundPeer := cfg.Outbound.M3DB
	outboundPeerAuth := make([]auth.OutboundCredentials, 0, len(nodeOutboundPeer.NodeConfig))
	for _, nodeCfg := range nodeOutboundPeer.NodeConfig {
		outboundPeerAuth = append(outboundPeerAuth, auth.OutboundCredentials{
			Username: *nodeCfg.Service.Username,
			Password: *nodeCfg.Service.Password,
			Zone:     nodeCfg.Service.Zone,
			Type:     auth.PeerCredential,
		})
	}

	nodeOutboundEtcd := cfg.Outbound.Etcd
	outboundEtcdAuth := make([]auth.OutboundCredentials, 0, len(nodeOutboundEtcd.NodeConfig))
	for _, nodeCfg := range nodeOutboundEtcd.NodeConfig {
		outboundEtcdAuth = append(outboundEtcdAuth, auth.OutboundCredentials{
			Username: *nodeCfg.Service.Username,
			Password: *nodeCfg.Service.Password,
			Zone:     nodeCfg.Service.Zone,
			Type:     auth.EtcdCredential,
		})
	}
	auth.PopulateOutbound(outboundPeerAuth, outboundEtcdAuth)
}

func parseAuthMode(str string) auth.Mode {
	c, ok := authModeMap[strings.ToLower(str)]
	if !ok {
		return auth.AuthModeUnknown
	}
	return c
}
