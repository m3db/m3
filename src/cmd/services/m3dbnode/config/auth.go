package config

import "fmt"

// AuthConfig is the top level config that includes secrets of inbound and
// outbound of a dbnode.
type AuthConfig struct {
	Outbound *Outbounds `yaml:"outbounds"`
	Inbound  *Inbound   `yaml:"inbounds"`
}

// Validate validates the AuthConfig. We use this method to validate fields
// where the validator package falls short.
func (c *AuthConfig) Validate() error {
	if c.Outbound == nil {
		return fmt.Errorf("outbound creds are not present")
	}

	if err := c.Outbound.Validate(); err != nil {
		return err
	}

	if c.Inbound != nil {
		if err := c.Inbound.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Outbounds consist secrets for outbounds connections for m3dbnodes and
// etcd.
type Outbounds struct {
	M3DB *ServicesConfig `yaml:"m3db"`
	Etcd *ServicesConfig `yaml:"etcd"`
}

// Validate validates the Outbounds.
func (c *Outbounds) Validate() error {
	if c.M3DB == nil {
		return fmt.Errorf("incomplete outbound creds, m3db node creds not provided")
	}

	if err := c.M3DB.Validate(); err != nil {
		return err
	}

	if c.Etcd == nil {
		return fmt.Errorf("incomplete outbound creds, etcd node creds not provided")
	}
	if err := c.Etcd.Validate(); err != nil {
		return err
	}

	return nil
}

// Inbound consist secrets for Inbound connections for m3dbnodes.
type Inbound struct {
	M3DB *InboundConfig `yaml:"m3db"`
}

// Validate validates the Inbound.
func (c *Inbound) Validate() error {
	if c.M3DB == nil {
		return fmt.Errorf("incomplete inboundcreds, m3db config not provided")
	}

	if err := c.M3DB.Validate(); err != nil {
		return err
	}
	return nil
}

// InboundConfig consist credentials and auth mode for inbound request.
type InboundConfig struct {
	Mode        *string               `yaml:"mode"`
	Credentials []*InboundCredentials `yaml:"credentials"`
}

// Validate validates the InboundConfig.
func (c *InboundConfig) Validate() error {
	if c.Mode == nil {
		return fmt.Errorf("incomplete inboundcreds, mode not provided")
	}

	for _, node := range c.Credentials {
		if err := node.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ServicesConfig consist of service level config.
type ServicesConfig struct {
	NodeConfig []*ServiceConfig `yaml:"services"`
}

// Validate validates the ServicesConfig.
func (c *ServicesConfig) Validate() error {
	if c.NodeConfig == nil || len(c.NodeConfig) == 0 {
		return fmt.Errorf("incomplete creds, service level creds not provided")
	}

	for _, node := range c.NodeConfig {
		if err := node.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// ServiceConfig consist of service level config.
type ServiceConfig struct {
	Service *OutboundCredentials `yaml:"service"`
}

// Validate validates the ServiceConfig.
func (c *ServiceConfig) Validate() error {
	if c.Service == nil {
		return fmt.Errorf("incomplete creds, service level creds not provided")
	}

	if err := c.Service.Validate(); err != nil {
		return err
	}

	return nil
}

// OutboundCredentials is the bottom most struct consist used for storing outbounds creds.
type OutboundCredentials struct {
	Zone     string
	Username *string
	Password *string
}

// Validate validates the Credentials.
func (c *OutboundCredentials) Validate() error {
	if c.Username == nil {
		return fmt.Errorf("incomplete creds, username not provided for outbounds")
	}
	if c.Password == nil {
		return fmt.Errorf("incomplete creds, password not provided for outbounds")
	}

	return nil
}

// InboundCredentials is the bottom most struct consist used for storing outbounds creds.
type InboundCredentials struct {
	Zone     string  `yaml:"zone"`
	Username *string `yaml:"username"`
	Digest   *string `yaml:"digest"`
}

// Validate validates the Credentials.
func (c *InboundCredentials) Validate() error {
	if c.Username == nil {
		return fmt.Errorf("incomplete creds, username not provided for inbounds")
	}
	if c.Digest == nil {
		return fmt.Errorf("incomplete creds, digest password not provided for inbounds")
	}

	return nil
}
