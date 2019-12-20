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

package serve

import (
	"fmt"

	"github.com/m3db/m3/src/aggregator/aggregator"
	httpserver "github.com/m3db/m3/src/aggregator/server/http"
	m3msgserver "github.com/m3db/m3/src/aggregator/server/m3msg"
	rawtcpserver "github.com/m3db/m3/src/aggregator/server/rawtcp"
	"github.com/m3db/m3/src/x/sampler"
	"github.com/m3db/m3/src/x/instrument"
)

var (
	defaultSampleRate = 0.01
)

// Serve starts serving RPC traffic.
func Serve(
	rawTCPAddr string,
	rawTCPServerOpts rawtcpserver.Options,
	httpAddr string,
	httpServerOpts httpserver.Options,
	m3msgAddr string,
	m3msgServerOpts m3msgserver.Options,
	aggregator aggregator.Aggregator,
	doneCh chan struct{},
	iOpts instrument.Options,
) error {
	log := rawTCPServerOpts.InstrumentOptions().Logger().Sugar()
	defer aggregator.Close()

	rawTCPServer := rawtcpserver.NewServer(rawTCPAddr, aggregator, rawTCPServerOpts)
	if err := rawTCPServer.ListenAndServe(); err != nil {
		return fmt.Errorf("could not start raw TCP server at %s: %v", rawTCPAddr, err)
	}
	defer rawTCPServer.Close()
	log.Infof("raw TCP server: listening on %s", rawTCPAddr)

	httpServer := httpserver.NewServer(httpAddr, aggregator, httpServerOpts, iOpts)
	if err := httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("could not start http server at %s: %v", httpAddr, err)
	}
	defer httpServer.Close()
	log.Infof("http server: listening on %s", httpAddr)

	if err := m3msgServerOpts.Validate(); err != nil {
		return err
	}
	if m3msgConfig := m3msgServerOpts.M3MsgConfiguration(); m3msgConfig != nil {
		s, err := sampler.NewSampler(defaultSampleRate)
		if err != nil {
			return fmt.Errorf("could not create sampler for m3msg server: %v", err)
		}
		m3msgServer, err := m3msgConfig.NewServer(
			m3msgserver.NewPassThroughWriteFn(aggregator, s, log.Desugar()),
			m3msgServerOpts.InstrumentOptions(),
		)
		if err != nil {
			return fmt.Errorf("could not create m3msg server at %s: %v", m3msgAddr, err)
		}
		if err := m3msgServer.ListenAndServe(); err != nil {
			return fmt.Errorf("could not start m3msg server at %s: %v", m3msgAddr, err)
		}
		defer m3msgServer.Close()
		log.Infof("m3msg server: listening on %s", m3msgAddr)
	}

	// Wait for exit signal.
	<-doneCh

	return nil
}
