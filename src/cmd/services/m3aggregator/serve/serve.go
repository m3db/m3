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
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	httpserver "github.com/m3db/m3/src/aggregator/server/http"
	m3msgserver "github.com/m3db/m3/src/aggregator/server/m3msg"
	rawtcpserver "github.com/m3db/m3/src/aggregator/server/rawtcp"
	xdebug "github.com/m3db/m3/src/x/debug"

	"go.uber.org/zap"
)

// Serve starts serving RPC traffic.
func Serve(
	aggregator aggregator.Aggregator,
	doneCh chan struct{},
	opts Options,
) error {
	iOpts := opts.InstrumentOpts()
	log := iOpts.Logger()
	defer func() {
		start := time.Now()
		log.Info("closing aggregator")
		err := aggregator.Close()
		fields := []zap.Field{zap.String("took", time.Since(start).String())}
		if err != nil {
			log.Warn("closed aggregator with error", append(fields, zap.Error(err))...)
		} else {
			log.Info("closed aggregator", fields...)
		}
	}()

	if m3msgAddr := opts.M3MsgAddr(); m3msgAddr != "" {
		serverOpts := opts.M3MsgServerOpts()
		m3msgServer, err := m3msgserver.NewServer(m3msgAddr, aggregator, serverOpts)
		if err != nil {
			return fmt.Errorf("could not create m3msg server: addr=%s, err=%v", m3msgAddr, err)
		}
		if err := m3msgServer.ListenAndServe(); err != nil {
			return fmt.Errorf("could not start m3msg server at: addr=%s, err=%v", m3msgAddr, err)
		}

		defer func() {
			start := time.Now()
			log.Info("closing m3msg server")
			m3msgServer.Close()
			log.Info("m3msg server closed", zap.String("took", time.Since(start).String()))
		}()

		log.Info("m3msg server listening", zap.String("addr", m3msgAddr))
	}

	if rawTCPAddr := opts.RawTCPAddr(); rawTCPAddr != "" {
		serverOpts := opts.RawTCPServerOpts()
		rawTCPServer := rawtcpserver.NewServer(rawTCPAddr, aggregator, serverOpts)
		if err := rawTCPServer.ListenAndServe(); err != nil {
			return fmt.Errorf("could not start raw TCP server at: addr=%s, err=%v", rawTCPAddr, err)
		}

		defer func() {
			start := time.Now()
			log.Info("closing raw TCPServer")
			rawTCPServer.Close()
			log.Info("closed raw TCPServer", zap.String("took", time.Since(start).String()))
		}()

		log.Info("raw TCP server listening", zap.String("addr", rawTCPAddr))
	}

	if httpAddr := opts.HTTPAddr(); httpAddr != "" {
		serverOpts := opts.HTTPServerOpts()
		xdebug.RegisterPProfHandlers(serverOpts.Mux())
		httpServer := httpserver.NewServer(httpAddr, aggregator, serverOpts, iOpts)
		if err := httpServer.ListenAndServe(); err != nil {
			return fmt.Errorf("could not start http server at: addr=%s, err=%v", httpAddr, err)
		}

		defer func() {
			start := time.Now()
			log.Info("closing http server")
			httpServer.Close()
			log.Info("closed http server", zap.String("took", time.Since(start).String()))
		}()

		log.Info("http server listening", zap.String("addr", httpAddr))
	}

	// Wait for exit signal.
	<-doneCh
	log.Info("server signalled on doneCh")

	return nil
}
