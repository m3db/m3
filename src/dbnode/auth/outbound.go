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
