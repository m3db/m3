package auth

import (
	"errors"
)

// Credentials encapsulates inbound and outbound credentials.
type Credentials struct {
	InboundCredentials
	OutboundCredentials
}

// InboundCredentials encapsulates credentials for inbound RPC to dbnode.
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

// OutboundCredentials encapsulates credentials for outbiund RPC from dbnode.
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
