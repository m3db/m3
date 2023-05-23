package auth

import (
	"github.com/uber/tchannel-go/thrift"
)

// Outbound encapsulates outbound credentials.
type Outbound struct {
	peerCredentials []OutboundCredentials
	etcdCredentials []OutboundCredentials
}

// WrapThriftContextWithPeerCreds wraps thrift context with outbound peer credential
// depending on CredentialType and zone.
func (o *Outbound) WrapThriftContextWithPeerCreds(tctx thrift.Context, zone string) thrift.Context {
	for _, peerCred := range o.peerCredentials {
		if peerCred.Zone == zone {
			return wrapTctxWithCredentials(tctx, peerCred)
		}
	}
	return tctx
}

func wrapTctxWithCredentials(tCtx thrift.Context, creds OutboundCredentials) thrift.Context {
	return thrift.WithHeaders(tCtx, map[string]string{
		AuthUsername: creds.Username,
		AuthPassword: creds.Password},
	)
}

// FetchOutboundEtcdCredentials fetches outbound etcd credentials.
func (o *Outbound) FetchOutboundEtcdCredentials(zone string) *OutboundCredentials {
	for _, peerCred := range o.etcdCredentials {
		if peerCred.Zone == zone {
			return &OutboundCredentials{
				Username: peerCred.Username,
				Password: peerCred.Password,
				Zone:     zone,
			}
		}
	}
	return nil
}
