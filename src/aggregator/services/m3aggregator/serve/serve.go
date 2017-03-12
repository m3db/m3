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

	"github.com/m3db/m3aggregator/aggregator"
	httpserver "github.com/m3db/m3aggregator/server/http"
	msgpackserver "github.com/m3db/m3aggregator/server/msgpack"
)

// Serve starts serving RPC traffic
func Serve(
	msgpackAddr string,
	msgpackServerOpts msgpackserver.Options,
	httpAddr string,
	httpServerOpts httpserver.Options,
	aggregator aggregator.Aggregator,
	doneCh chan struct{},
) error {
	log := msgpackServerOpts.InstrumentOptions().Logger()
	defer aggregator.Close()

	msgpackServer := msgpackserver.NewServer(msgpackAddr, aggregator, msgpackServerOpts)
	if err := msgpackServer.ListenAndServe(); err != nil {
		return fmt.Errorf("could not start msgpack server at %s: %v", msgpackAddr, err)
	}
	defer msgpackServer.Close()
	log.Infof("msgpack server: listening on %s", msgpackAddr)

	httpServer := httpserver.NewServer(httpAddr, aggregator, httpServerOpts)
	if err := httpServer.ListenAndServe(); err != nil {
		return fmt.Errorf("could not start http server at %s: %v", httpAddr, err)
	}
	defer httpServer.Close()
	log.Infof("http server: listening on %s", httpAddr)

	// Wait for exit signal
	<-doneCh

	return nil
}
