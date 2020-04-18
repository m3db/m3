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
	"github.com/m3db/m3/src/x/instrument"

	"go.uber.org/zap"
)

// Serve starts serving RPC traffic.
func Serve(
	m3msgAddr string,
	m3msgServerOpts m3msgserver.Options,
	rawTCPAddr string,
	rawTCPServerOpts rawtcpserver.Options,
	httpAddr string,
	httpServerOpts httpserver.Options,
	aggregator aggregator.Aggregator,
	doneCh chan struct{},
	iOpts instrument.Options,
) error {
	log := iOpts.Logger()
	defer aggregator.Close()

	if m3msgAddr != "" {
		m3msgServer, err := m3msgserver.NewServer(m3msgAddr, aggregator, m3msgServerOpts)
		if err != nil {
			return fmt.Errorf("could not create m3msg server: addr=%s, err=%v", m3msgAddr, err)
		}
		if err := m3msgServer.ListenAndServe(); err != nil {
			return fmt.Errorf("could not start m3msg server at: addr=%s, err=%v", m3msgAddr, err)
		}
		defer m3msgServer.Close()
		log.Info("m3msg server listening", zap.String("addr", m3msgAddr))
	}

	if rawTCPAddr != "" {
		rawTCPServer := rawtcpserver.NewServer(rawTCPAddr, aggregator, rawTCPServerOpts)
		if err := rawTCPServer.ListenAndServe(); err != nil {
			return fmt.Errorf("could not start raw TCP server at: addr=%s, err=%v", rawTCPAddr, err)
		}
		defer rawTCPServer.Close()
		log.Info("raw TCP server listening", zap.String("addr", rawTCPAddr))
	}

	if httpAddr != "" {
		httpServer := httpserver.NewServer(httpAddr, aggregator, httpServerOpts, iOpts)
		if err := httpServer.ListenAndServe(); err != nil {
			return fmt.Errorf("could not start http server at: addr=%s, err=%v", httpAddr, err)
		}
		defer httpServer.Close()
		log.Info("http server listening", zap.String("addr", httpAddr))
	}

	// Wait for exit signal.
	<-doneCh

	return nil
}
