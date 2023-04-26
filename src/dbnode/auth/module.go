package auth

import (
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"strings"
	"sync"
)

const (
	// AUTH_PASSWORD defines password key in thrift ctx.
	AUTH_PASSWORD = "password"

	// AUTH_USERNAME defines username key in thrift ctx.
	AUTH_USERNAME = "username"

	// AUTH_ZONE defines zone key in thrift ctx.
	AUTH_ZONE = "zone"
)

var (
	// InboundAuth encapsulates inbound credentials for the dbnode.
	InboundAuth *Inbound
	inboundLock = &sync.Mutex{}

	// OutboundAuth encapsulates outbound credentials for dbnode and client.
	OutboundAuth *Outbound

	authModeMap = map[string]AuthMode{
		"none":    AuthModeNoAuth,
		"shadow":  AuthModeShadow,
		"enabled": AuthModeEnforced,
	}
)

// PopulateDefaultAuthConfig populates default auth config in case of no AuthConfig is present.
func PopulateDefaultAuthConfig() {
	InboundAuth = &Inbound{authMode: AuthModeNoAuth}
	OutboundAuth = &Outbound{}
}

// PopulateInboundAuthConfig populates inbound auth modules with the provided auth config.
func PopulateInboundAuthConfig(cfg config.AuthConfig) {
	var inboundAuth []Credentials
	nodeInbound := cfg.Inbound.M3DB
	for _, nodeCfg := range nodeInbound.Credentials {
		inboundAuth = append(inboundAuth, Credentials{
			InboundCredentials: InboundCredentials{
				Username: *nodeCfg.Username,
				Digest:   *nodeCfg.Digest,
				Type:     ClientCredential,
			},
		})
	}
	authMode := parseAuthMode(*cfg.Inbound.M3DB.Mode)

	InboundAuth = &Inbound{clientCredentials: inboundAuth, authMode: authMode}
}

// RefreshInboundAuthConfig take AuthConfig as input param and populates inbound credentials global module.
func RefreshInboundAuthConfig(credentialsConfig config.AuthConfig) {
	inboundLock.Lock()
	defer inboundLock.Unlock()
	PopulateInboundAuthConfig(credentialsConfig)
}

// PopulateOutboundAuthConfig populates outbound auth modules with the provided auth config.
func PopulateOutboundAuthConfig(cfg config.AuthConfig) {
	var outboundPeerAuth []Credentials
	nodeOutboundPeer := cfg.Outbound.M3DB

	for _, nodeCfg := range nodeOutboundPeer.NodeConfig {
		outboundPeerAuth = append(outboundPeerAuth, Credentials{
			OutboundCredentials: OutboundCredentials{
				Username: *nodeCfg.Service.Username,
				Password: *nodeCfg.Service.Password,
				Zone:     nodeCfg.Service.Zone,
				Type:     PeerCredential,
			},
		})
	}

	var outboundEtcdAuth []Credentials
	nodeOutboundEtcd := cfg.Outbound.Etcd

	for _, nodeCfg := range nodeOutboundEtcd.NodeConfig {
		outboundEtcdAuth = append(outboundEtcdAuth, Credentials{
			OutboundCredentials: OutboundCredentials{
				Username: *nodeCfg.Service.Username,
				Password: *nodeCfg.Service.Password,
				Zone:     nodeCfg.Service.Zone,
				Type:     EtcdCredential,
			},
		})
	}

	OutboundAuth = &Outbound{peerCredentials: outboundPeerAuth, etcdCredentials: outboundEtcdAuth}
}

func parseAuthMode(str string) AuthMode {
	c, ok := authModeMap[strings.ToLower(str)]
	if !ok {
		return AuthModeUnknown
	}
	return c
}
