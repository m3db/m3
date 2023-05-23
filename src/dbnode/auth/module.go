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
