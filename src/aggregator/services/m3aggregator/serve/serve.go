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
	"github.com/m3db/m3aggregator/server"
)

// Serve starts serving RPC traffic
func Serve(
	listenAddr string,
	serverOpts server.Options,
	aggregator aggregator.Aggregator,
	aggregatorOpts aggregator.Options,
	doneCh chan struct{},
) error {
	log := serverOpts.InstrumentOptions().Logger()
	s := server.NewServer(listenAddr, aggregator, serverOpts)

	closer, err := s.ListenAndServe()
	if err != nil {
		return fmt.Errorf("could not listen on %s: %v", listenAddr, err)
	}
	log.Infof("start listening on %s", listenAddr)

	// Wait for exit signal
	<-doneCh

	log.Debug("closing the server")
	closer.Close()

	return nil
}
