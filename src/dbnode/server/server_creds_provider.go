package server

import (
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/auth"
	"strings"
)

var (
	authModeMap = map[string]auth.AuthMode{
		"none":    auth.AuthModeNoAuth,
		"shadow":  auth.AuthModeShadow,
		"enabled": auth.AuthModeEnforced,
	}
)

// PopulateInboundAuthConfig populates inbound auth modules with the provided auth config.
func PopulateInboundAuthConfig(cfg config.AuthConfig) {
	var inboundAuth []auth.InboundCredentials
	nodeInbound := cfg.Inbound.M3DB
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
	var outboundPeerAuth []auth.OutboundCredentials
	nodeOutboundPeer := cfg.Outbound.M3DB

	for _, nodeCfg := range nodeOutboundPeer.NodeConfig {
		outboundPeerAuth = append(outboundPeerAuth, auth.OutboundCredentials{
			Username: *nodeCfg.Service.Username,
			Password: *nodeCfg.Service.Password,
			Zone:     nodeCfg.Service.Zone,
			Type:     auth.PeerCredential,
		})
	}

	var outboundEtcdAuth []auth.OutboundCredentials
	nodeOutboundEtcd := cfg.Outbound.Etcd

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

func parseAuthMode(str string) auth.AuthMode {
	c, ok := authModeMap[strings.ToLower(str)]
	if !ok {
		return auth.AuthModeUnknown
	}
	return c
}
