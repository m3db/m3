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

// CredentialType designates credentials for different connection edges.
type CredentialType int

const (
	// CredentialUnknown defines unknown connection edge.
	CredentialUnknown CredentialType = iota

	// CredentialClient defines m3db client to dbnode connection credentials.
	CredentialClient

	// CredentialPeer defines dbnode to dbnode connections credentials.
	CredentialPeer

	// CredentialEtcd defines dbnode to etcd connections credentials.
	CredentialEtcd
)

// Mode designates a type of authentication.
type Mode int

const (
	// ModeUnknown is unknown authentication type case.
	ModeUnknown Mode = iota

	// ModeNoAuth is no authentication type case.
	ModeNoAuth

	// ModeShadow mode runs authentication in shadow mode. Credentials will be passed
	// by respective peers/clients but will not be used to reject RPCs in case of auth failure.
	ModeShadow

	// ModeEnforced mode runs dbnode in enforced authentication mode. RPCs to dbnode will be rejected
	// if auth fails.
	ModeEnforced
)
