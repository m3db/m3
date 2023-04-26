package auth

import (
	"github.com/uber/tchannel-go/thrift"
)

// Outbound encapsulates outbound credentials.
type Outbound struct {
	peerCredentials []Credentials
	etcdCredentials []Credentials
}

// WrapThriftContextWithPeerCreds wraps thrift context with outbound peer credential depending on CredentialType and zone.
func (o *Outbound) WrapThriftContextWithPeerCreds(tctx thrift.Context, zone string) thrift.Context {
	for _, peerCred := range o.peerCredentials {
		if peerCred.OutboundCredentials.Zone == zone {
			return wrapTctxWithCredentials(tctx, peerCred)
		}
	}
	return tctx
}

func wrapTctxWithCredentials(tCtx thrift.Context, creds Credentials) thrift.Context {
	return thrift.WithHeaders(tCtx, map[string]string{
		AUTH_USERNAME: creds.OutboundCredentials.Username,
		AUTH_PASSWORD: creds.OutboundCredentials.Password},
	)
}

// FetchOutboundEtcdCredentials fetches outbound etcd credentials.
func (o *Outbound) FetchOutboundEtcdCredentials(zone string) *OutboundCredentials {
	for _, peerCred := range o.etcdCredentials {
		if peerCred.OutboundCredentials.Zone == zone {
			return &OutboundCredentials{
				Username: peerCred.OutboundCredentials.Username,
				Password: peerCred.OutboundCredentials.Password,
				Zone:     zone,
			}
		}
	}
	return nil
}
