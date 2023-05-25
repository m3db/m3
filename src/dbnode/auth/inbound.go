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
	"fmt"

	"github.com/uber/tchannel-go/thrift"
)

// Inbound encapsulates client credentials.
type Inbound struct {
	clientCredentials []InboundCredentials
	authMode          Mode
}

// ValidateCredentials validates the inbound credential and return error accordingly.
func (i *Inbound) ValidateCredentials(creds InboundCredentials) error {
	if i == nil || i.authMode == AuthModeNoAuth {
		return nil
	}

	if i.authMode == AuthModeShadow {
		go func() {
			credentialMatched := false
			for _, p := range i.clientCredentials {
				if i.MatchCredentials(p, creds) {
					credentialMatched = true
					break
				}
			}

			if !credentialMatched {
				// todo emit a metric
			}
		}()
		return nil
	}
	if creds.Type == Unknown {
		return fmt.Errorf("unknown credential type for dbnode inbound")
	}

	if creds.Type != ClientCredential {
		return fmt.Errorf("incorrect credential type for dbnode inbound")
	}

	if err := creds.Validate(); err != nil {
		return err
	}
	for _, p := range i.clientCredentials {
		if i.MatchCredentials(p, creds) {
			return nil
		}
	}
	return fmt.Errorf("credential not matched for dbnode inbound")
}

// ValidateCredentialsFromThriftContext validates inbound credential from thrift context.
func (i *Inbound) ValidateCredentialsFromThriftContext(tctx thrift.Context, credtype CredentialType) error {
	ctxHeaders := tctx.Headers()
	userName, ok := ctxHeaders[AuthUsername]
	if !ok {
		userName = ""
	}

	password, ok := ctxHeaders[AuthPassword]
	if !ok {
		password = ""
	}
	// todo create digest from the password and pass to handler.
	return i.ValidateCredentials(
		InboundCredentials{
			Username: userName,
			Digest:   password,
			Type:     credtype,
		},
	)
}

// MatchCredentials compares two inbound credentials and error out accordingly.
func (i *Inbound) MatchCredentials(c1, c2 InboundCredentials) bool {
	if c1.Username == c2.Username && c1.Digest == c2.Digest {
		return true
	}
	return false
}
