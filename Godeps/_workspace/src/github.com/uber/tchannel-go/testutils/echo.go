// Copyright (c) 2015 Uber Technologies, Inc.

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

package testutils

import (
	"time"

	"golang.org/x/net/context"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/raw"
)

// CallEcho calls the "echo" endpoint from the given src to target.
func CallEcho(src *tchannel.Channel, targetHostPort, targetService string, args *raw.Args) error {
	ctx, cancel := tchannel.NewContext(Timeout(300 * time.Millisecond))
	defer cancel()

	if args == nil {
		args = &raw.Args{}
	}

	_, _, _, err := raw.Call(ctx, src, targetHostPort, targetService, "echo", args.Arg2, args.Arg3)
	return err
}

// RegisterEcho registers an echo endpoint on the given channel. The optional provided
// function is run before the handler returns.
func RegisterEcho(src *tchannel.Channel, f func()) {
	RegisterFunc(src, "echo", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
		if f != nil {
			f()
		}
		return &raw.Res{Arg2: args.Arg2, Arg3: args.Arg3}, nil
	})
}

// Ping sends a ping from src to target.
func Ping(src, target *tchannel.Channel) error {
	ctx, cancel := tchannel.NewContext(Timeout(100 * time.Millisecond))
	defer cancel()

	return src.Ping(ctx, target.PeerInfo().HostPort)
}
