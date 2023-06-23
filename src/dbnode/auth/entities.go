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
	"errors"
)

// InboundCredentials encapsulates credentials for inbound RPCs to dbnode.
type InboundCredentials struct {
	Username string
	Digest   string
	Type     CredentialType
}

// Validate validates the InboundCredentials.
func (c *InboundCredentials) Validate() error {
	if c.Username == "" {
		return errors.New("username field is empty for inbound")
	}

	if c.Digest == "" {
		return errors.New("digest field is empty for inbound")
	}

	if c.Type != ClientCredential {
		return errors.New("incorrect cred type field for inbound")
	}

	return nil
}

// OutboundCredentials encapsulates credentials for outbound RPCs from dbnode.
type OutboundCredentials struct {
	Username string
	Password string
	Zone     string
	Type     CredentialType
}

// Validate validates the OutboundCredentials.
func (c *OutboundCredentials) Validate() error {
	if c.Username == "" {
		return errors.New("username field is empty for outbound")
	}

	if c.Password == "" {
		return errors.New("password field is empty for outbound")
	}

	if c.Type == Unknown || c.Type == ClientCredential {
		return errors.New("incorrect cred type field for outbound")
	}

	if c.Zone == "" {
		return errors.New("zone field is not set for outbound")
	}
	return nil
}
