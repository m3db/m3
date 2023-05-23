package auth

import (
	"sync"
)

const (
	// AuthPassword defines password key in thrift ctx.
	AuthPassword = "password"

	// AuthUsername defines username key in thrift ctx.
	AuthUsername = "username"

	// AuthZone defines zone key in thrift ctx.
	AuthZone = "zone"
)

var (
	// InboundAuth encapsulates inbound credentials for the dbnode.
	InboundAuth *Inbound
	// InboundLock encapsulates mutual exclusion lock, used while refreshing creds.
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
func PopulateInbound(clientCredentials []InboundCredentials, authMode Mode) {
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
