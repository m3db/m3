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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/auth"
	"github.com/m3db/m3/src/dbnode/auth/integration"

	"github.com/stretchr/testify/assert"
	"github.com/uber/tchannel-go/thrift"
)

func TestInboundCfgPopulate(t *testing.T) {
	defer inboundAuthCleanup()
	authCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithAuthEnabled)

	PopulateInboundAuthConfig(*authCfg)

	for _, nodeCfg := range authCfg.Inbound.M3DB.Credentials {
		err := auth.InboundAuth.ValidateCredentials(auth.InboundCredentials{
			Username: *nodeCfg.Username,
			Digest:   *nodeCfg.Digest,
			Type:     auth.ClientCredential,
		})
		assert.NoError(t, err)
	}
}

func TestInboundCfgPopulateWithRefresh(t *testing.T) {
	defer inboundAuthCleanup()
	authCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithAuthEnabled)

	PopulateInboundAuthConfig(*authCfg)

	for _, nodeCfg := range authCfg.Inbound.M3DB.Credentials {
		err := auth.InboundAuth.ValidateCredentials(auth.InboundCredentials{
			Username: *nodeCfg.Username,
			Digest:   *nodeCfg.Digest,
			Type:     auth.ClientCredential,
		})
		assert.NoError(t, err)
	}

	refreshAuthCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithAuthEnabledRefresh)
	RefreshInboundAuthConfig(*refreshAuthCfg)

	for _, nodeCfg := range refreshAuthCfg.Inbound.M3DB.Credentials {
		err := auth.InboundAuth.ValidateCredentials(auth.InboundCredentials{
			Username: *nodeCfg.Username,
			Digest:   *nodeCfg.Digest,
			Type:     auth.ClientCredential,
		})
		assert.NoError(t, err)
	}
}

func TestInboundCfgPopulateWithIncorrectCredsAuthDisabled(t *testing.T) {
	defer inboundAuthCleanup()
	authCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithAuthDisabled)
	PopulateInboundAuthConfig(*authCfg)

	for _, nodeCfg := range authCfg.Inbound.M3DB.Credentials {
		err := auth.InboundAuth.ValidateCredentials(auth.InboundCredentials{
			Username: "some_random_u",
			Digest:   *nodeCfg.Digest,
			Type:     auth.ClientCredential,
		})
		assert.NoError(t, err)
	}

	refreshAuthCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithAuthEnabledRefresh)
	RefreshInboundAuthConfig(*refreshAuthCfg)

	for _, nodeCfg := range authCfg.Inbound.M3DB.Credentials {
		err := auth.InboundAuth.ValidateCredentials(auth.InboundCredentials{
			Username: *nodeCfg.Username,
			Digest:   "some_random_p",
			Type:     auth.ClientCredential,
		})
		assert.Error(t, err)
	}
}

func TestInboundCfgPopulateWithCorrectCredsAuthDisabledRefreshAuthEnabled(t *testing.T) {
	defer inboundAuthCleanup()
	authCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithAuthDisabled)
	PopulateInboundAuthConfig(*authCfg)

	for _, nodeCfg := range authCfg.Inbound.M3DB.Credentials {
		err := auth.InboundAuth.ValidateCredentials(auth.InboundCredentials{
			Username: "some_random_u",
			Digest:   *nodeCfg.Digest,
			Type:     auth.ClientCredential,
		})
		assert.NoError(t, err)
	}

	refreshAuthCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithAuthEnabledRefresh)
	RefreshInboundAuthConfig(*refreshAuthCfg)

	for _, nodeCfg := range refreshAuthCfg.Inbound.M3DB.Credentials {
		err := auth.InboundAuth.ValidateCredentials(auth.InboundCredentials{
			Username: *nodeCfg.Username,
			Digest:   *nodeCfg.Digest,
			Type:     auth.ClientCredential,
		})
		assert.NoError(t, err)
	}
}

func TestPeerOutboundCfgPopulate(t *testing.T) {
	defer outboundAuthCleanup()
	authCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithOutbounds)

	PopulateOutboundAuthConfig(*authCfg)
	tCtx, _ := thrift.NewContext(time.Minute)

	for _, nodeCfg := range authCfg.Outbound.M3DB.NodeConfig {
		newTctx := auth.OutboundAuth.WrapThriftContextWithPeerCreds(tCtx, nodeCfg.Service.Zone)
		assert.Equal(t, *nodeCfg.Service.Username, newTctx.Headers()["username"])
		assert.Equal(t, *nodeCfg.Service.Password, newTctx.Headers()["password"])
	}
}

func TestEtcdOutboundCfgPopulate(t *testing.T) {
	defer outboundAuthCleanup()
	authCfg := integration.CreateTestConfigYaml(integration.BaseConfigWithOutbounds)

	PopulateOutboundAuthConfig(*authCfg)

	for _, nodeCfg := range authCfg.Outbound.Etcd.NodeConfig {
		newTctx := auth.OutboundAuth.FetchOutboundEtcdCredentials(nodeCfg.Service.Zone)
		assert.Equal(t, *nodeCfg.Service.Username, newTctx.Username)
		assert.Equal(t, *nodeCfg.Service.Password, newTctx.Password)
		assert.Equal(t, nodeCfg.Service.Zone, newTctx.Zone)
	}
}

func inboundAuthCleanup() {
	auth.InboundAuth = &auth.Inbound{}
}

func outboundAuthCleanup() {
	auth.OutboundAuth = &auth.Outbound{}
}
