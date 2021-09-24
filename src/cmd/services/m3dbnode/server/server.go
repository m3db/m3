// Copyright (c) 2021  Uber Technologies, Inc.
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

// Package server is a package for starting servers for M3 components.
package server

import (
	"errors"
	"time"

	xos "github.com/m3db/m3/src/x/os"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/client"
	dbserver "github.com/m3db/m3/src/dbnode/server"
	coordinatorserver "github.com/m3db/m3/src/query/server"
)

// Options contains options for starting M3 components.
type Options struct {
	// Configuration is the top level configuration that includes both a DB
	// node and a coordinator.
	Configuration config.Configuration

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh <-chan error

	// ShutdownCh is an optional channel to supply if interested in receiving
	// a notification that the server has shutdown.
	ShutdownCh chan<- struct{}
}

// RunComponents runs the appropriate M3 components based on the configuration.
// Kicks off either a single DB node or both a DB node and coordinator.
func RunComponents(opts Options) {
	var (
		cfg         = opts.Configuration
		interruptCh = opts.InterruptCh
		shutdownCh  = opts.ShutdownCh

		dbClientCh        chan client.Client
		clusterClientCh   chan clusterclient.Client
		coordinatorDoneCh chan struct{}
	)

	if cfg.DB != nil {
		dbClientCh = make(chan client.Client, 1)
		clusterClientCh = make(chan clusterclient.Client, 1)
	}

	if cfg.Coordinator != nil {
		coordinatorDoneCh = make(chan struct{}, 1)
		go func() {
			coordinatorserver.Run(coordinatorserver.RunOptions{
				Config:        *cfg.Coordinator,
				DBConfig:      cfg.DB,
				DBClient:      dbClientCh,
				ClusterClient: clusterClientCh,
				InterruptCh:   interruptCh,
				ShutdownCh:    shutdownCh,
			})
			coordinatorDoneCh <- struct{}{}
		}()
	}

	if cfg.DB != nil {
		dbserver.Run(dbserver.RunOptions{
			Config:          *cfg.DB,
			ClientCh:        dbClientCh,
			ClusterClientCh: clusterClientCh,
			InterruptCh:     interruptCh,
			ShutdownCh:      shutdownCh,
		})
	} else if cfg.Coordinator != nil {
		<-coordinatorDoneCh
	}
}

const (
	interruptTimeout = 5 * time.Second
	shutdownTimeout  = time.Minute
)

// CloseOptions are options for closing the started components.
type CloseOptions struct {
	// Configuration is the top level configuration that includes both a DB
	// node and a coordinator.
	Configuration config.Configuration

	// InterruptCh is a programmatic interrupt channel to supply to
	// interrupt and shutdown the server.
	InterruptCh chan<- error

	// ShutdownCh is an optional channel to supply if interested in receiving
	// a notification that the server has shutdown.
	ShutdownCh <-chan struct{}
}

// Close programmatically closes components that were started via RunComponents.
func Close(opts CloseOptions) error {
	var (
		cfg         = opts.Configuration
		interruptCh = opts.InterruptCh
		shutdownCh  = opts.ShutdownCh
	)

	if err := interrupt(cfg, interruptCh); err != nil {
		return err
	}

	if err := waitForShutdown(cfg, shutdownCh); err != nil {
		return err
	}

	return nil
}

func interrupt(cfg config.Configuration, interruptCh chan<- error) error {
	if cfg.Coordinator != nil {
		select {
		case interruptCh <- xos.NewInterruptError("interrupt received. shutting down"):
		case <-time.After(interruptTimeout):
			return errors.New("timeout sending interrupt. server may not close")
		}
	}

	if cfg.DB != nil {
		select {
		case interruptCh <- xos.NewInterruptError("interrupt received. shutting down"):
		case <-time.After(interruptTimeout):
			return errors.New("timeout sending interrupt. server may not close")
		}
	}

	return nil
}

func waitForShutdown(cfg config.Configuration, shutdownCh <-chan struct{}) error {
	if cfg.Coordinator != nil {
		select {
		case <-shutdownCh:
		case <-time.After(shutdownTimeout):
			return errors.New("timeout waiting for shutdown notification. closing may" +
				" not be completely graceful")
		}
	}

	if cfg.DB != nil {
		select {
		case <-shutdownCh:
		case <-time.After(shutdownTimeout):
			return errors.New("timeout waiting for shutdown notification. closing may" +
				" not be completely graceful")
		}
	}

	return nil
}
