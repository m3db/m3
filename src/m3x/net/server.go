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

package xnet

import (
	"net"

	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/retry"
)

// StartAcceptLoop starts an accept loop for the given listener,
// returning accepted connections via a channel while handling
// temporary network errors. Fatal errors are returned via the
// error channel with the listener closed on return
func StartAcceptLoop(l net.Listener, retrier xretry.Retrier) (<-chan net.Conn, <-chan error) {
	var (
		connCh = make(chan net.Conn)
		errCh  = make(chan error)
	)

	go func() {
		defer l.Close()

		for {
			var conn net.Conn
			if err := retrier.Attempt(func() error {
				var connErr error
				conn, connErr = l.Accept()
				if connErr == nil {
					return nil
				}
				// If the error is a temporary network error, we consider it retryable
				if ne, ok := connErr.(net.Error); ok && ne.Temporary() {
					return ne
				}
				// Otherwise it's a non-retryable error
				return xerrors.NewNonRetryableError(connErr)
			}); err != nil {
				close(connCh)
				errCh <- err
				close(errCh)
				return
			}
			connCh <- conn
		}
	}()

	return connCh, errCh
}
