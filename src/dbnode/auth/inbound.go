package auth

import (
	"fmt"
	"github.com/uber/tchannel-go/thrift"
)

// Inbound encapsulates client credentials.
type Inbound struct {
	clientCredentials []Credentials
	authMode          AuthMode
}

// ValidateCredentials validates the inbound credential and return error accordingly.
func (i *Inbound) ValidateCredentials(creds Credentials) error {
	if i.authMode == AuthModeNoAuth {
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
	if creds.InboundCredentials.Type == Unknown {
		return fmt.Errorf("unknown credential type for dbnode inbound")
	}

	if creds.InboundCredentials.Type != ClientCredential {
		return fmt.Errorf("incorrect credential type for dbnode inbound")
	}

	if err := creds.InboundCredentials.Validate(); err != nil {
		return err
	}
	for _, p := range i.clientCredentials {
		if i.MatchCredentials(p, creds) {
			return nil
		}
	}
	return fmt.Errorf("credential not matched for dbnode inbound")
}

// ValidateCredentialsFromThriftContext validates inbound credential from thrift context. ValidateCredsFromTCtxHeaders checks for
// call's request headers .
func (i *Inbound) ValidateCredentialsFromThriftContext(tctx thrift.Context, credtype CredentialType) error {
	ctxHeaders := tctx.Headers()
	userName, ok := ctxHeaders[AUTH_USERNAME]
	if !ok {
		userName = ""
	}

	password, ok := ctxHeaders[AUTH_PASSWORD]
	if !ok {
		password = ""
	}
	// todo create digest from the password and pass to handler.
	return i.ValidateCredentials(Credentials{
		InboundCredentials: InboundCredentials{
			Username: userName,
			Digest:   password,
			Type:     credtype,
		},
	})
}

// MatchCredentials compares two inbound credentials and error out accordingly.
func (i *Inbound) MatchCredentials(c1, c2 Credentials) bool {
	if c1.InboundCredentials.Username == c2.InboundCredentials.Username && c1.InboundCredentials.Digest == c2.InboundCredentials.Digest {
		return true
	}
	return false
}
