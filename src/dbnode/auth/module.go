package auth

import (
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
	InboundLock = &sync.Mutex{}

	// OutboundAuth encapsulates outbound credentials for dbnode and client.
	OutboundAuth *Outbound
)

// PopulateDefaultAuthConfig populates default auth config in case of no AuthConfig is present.
func PopulateDefaultAuthConfig() {
	InboundAuth = &Inbound{authMode: AuthModeNoAuth}
	OutboundAuth = &Outbound{}
}

// PopulateInbound populates inbound credentials for dbnode.
func PopulateInbound(clientCredentials []InboundCredentials, authMode AuthMode) {
	InboundAuth = &Inbound{
		clientCredentials: clientCredentials,
		authMode:          authMode,
	}
}

// PopulateOutbound populates outbound credentials for dbnode/clients.
func PopulateOutbound(peerCredentials []OutboundCredentials, etcdCredentials []OutboundCredentials) {
	OutboundAuth = &Outbound{
		peerCredentials: peerCredentials,
		etcdCredentials: etcdCredentials,
	}
}
