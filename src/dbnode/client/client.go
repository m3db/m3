// Copyright (c) 2016 Uber Technologies, Inc.
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

package client

import (
	"fmt"
	"sync"

	csclient "github.com/m3db/m3/src/cluster/client"
	"go.uber.org/zap"
)

type client struct {
	sync.Mutex

	opts         Options
	asyncOpts    []Options
	newSessionFn newReplicatedSessionFn
	session      AdminSession // default cached session
}

// type newReplicatedSessionFn func(Options) (replicatedSession, error)
type newReplicatedSessionFn func(Options, []Options, ...replicatedSessionOption) (clientSession, error)

// NewClient creates a new client
func NewClient(opts Options, asyncOpts ...Options) (Client, error) {
	return newClient(opts, asyncOpts...)
}

// NewAdminClient creates a new administrative client
func NewAdminClient(opts AdminOptions, asyncOpts ...Options) (AdminClient, error) {
	opts.InstrumentOptions().Logger().Info("creating admin client3")
	return newClient(opts, asyncOpts...)
}

func newClient(opts Options, asyncOpts ...Options) (*client, error) {
	opts.InstrumentOptions().Logger().Info("creating admin client4")
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	opts.InstrumentOptions().Logger().Info("validated admin client")
	// Set up circuit breaker if possible
	opts = setupCircuitBreakerFromTopology(opts)

	return &client{opts: opts, asyncOpts: asyncOpts, newSessionFn: newReplicatedSession}, nil
}

// setupCircuitBreakerFromTopology attempts to set up circuit breaker middleware
// by extracting ConfigServiceClient from the topology initializer
func setupCircuitBreakerFromTopology(opts Options) Options {
	topologyInit := opts.TopologyInitializer()
	logger := opts.InstrumentOptions().Logger()
	logger.Info("setting up circuit breaker from topology")
	if topologyInit != nil {
		logger.Info("topology initializer is not nil")
		logger.Info("topology initializer type", zap.String("type", fmt.Sprintf("%T", topologyInit)))

		// Check if it's a dynamic initializer that has ConfigServiceClient
		logger.Info("attempting type assertion for ConfigServiceClient")
		if dynamicInit, ok := topologyInit.(interface {
			ConfigServiceClient() csclient.Client
		}); ok {
			logger.Info("dynamic initializer is not nil")
			configServiceClient := dynamicInit.ConfigServiceClient()
			logger.Info("config service client result", zap.Any("configServiceClient", configServiceClient))
			if configServiceClient != nil {
				logger.Info("config service client is not nil")
				// Get KV store from the config service client
				kvStore, err := configServiceClient.KV()
				if err != nil {
					// Log error but don't fail - circuit breaker is optional
					opts.InstrumentOptions().Logger().Warn("failed to get KV store from config service client", zap.Error(err))
					return opts
				}
				logger.Info("kv store is not nil")
				provider, err := SetupCircuitBreakerProvider(kvStore, opts.InstrumentOptions())
				if err != nil {
					// Log error but don't fail - circuit breaker is optional
					opts.InstrumentOptions().Logger().Warn("failed to set up circuit breaker provider", zap.Error(err))
					return opts
				}
				return opts.SetMiddlewareEnableProvider(provider)
			} else {
				logger.Info("config service client is nil")
			}
		} else {
			logger.Info("type assertion failed - topology initializer does not have ConfigServiceClient method")
		}
	} else {
		logger.Info("topology initializer is nil")
	}
	return opts
}

func (c *client) newSession(opts Options) (AdminSession, error) {
	session, err := c.newSessionFn(opts, c.asyncOpts)
	if err != nil {
		return nil, err
	}
	if err := session.Open(); err != nil {
		return nil, err
	}
	return session, nil
}

func (c *client) defaultSession() (AdminSession, error) {
	c.Lock()
	if c.session != nil {
		session := c.session
		c.Unlock()
		return session, nil
	}
	c.Unlock()

	session, err := c.newSession(c.opts)
	if err != nil {
		return nil, err
	}

	c.Lock()
	if c.session != nil {
		session := c.session
		c.Unlock()
		return session, nil
	}
	c.session = session
	c.Unlock()

	return session, nil
}

func (c *client) Options() Options {
	return c.opts
}

func (c *client) NewSession() (Session, error) {
	return c.newSession(c.opts)
}

func (c *client) NewSessionWithOptions(opts Options) (Session, error) {
	return c.newSession(opts)
}

func (c *client) DefaultSession() (Session, error) {
	return c.defaultSession()
}

func (c *client) NewAdminSession() (AdminSession, error) {
	return c.newSession(c.opts)
}

func (c *client) DefaultAdminSession() (AdminSession, error) {
	return c.defaultSession()
}

func (c *client) DefaultSessionActive() bool {
	c.Lock()
	defer c.Unlock()
	return c.session != nil
}
