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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go"
)

func getOptsForTest(t testing.TB, opts *ChannelOpts) *ChannelOpts {
	if opts == nil {
		opts = &ChannelOpts{}
	}
	opts.optFn = func(opts *ChannelOpts) {
		// Set a custom logger now.
		if opts.Logger == nil {
			tl := newTestLogger(t)
			opts.Logger = tl
			opts.addPostFn(func() {
				tl.report()
			})
		}
		if !opts.LogVerification.Disabled {
			opts.Logger = opts.LogVerification.WrapLogger(t, opts.Logger)
		}
	}
	return opts
}

// WithServer sets up a TChannel that is listening and runs the given function with the channel.
func WithServer(t testing.TB, opts *ChannelOpts, f func(ch *tchannel.Channel, hostPort string)) {
	opts = getOptsForTest(t, opts)
	ch := NewServer(t, opts)
	f(ch, ch.PeerInfo().HostPort)
	ch.Close()
}

// NewServer returns a new TChannel server that listens on :0.
func NewServer(t testing.TB, opts *ChannelOpts) *tchannel.Channel {
	opts = getOptsForTest(t, opts)
	ch, err := NewServerChannel(opts)
	require.NoError(t, err, "NewServerChannel failed")
	return ch
}

// NewClient returns a new TChannel that is not listening.
func NewClient(t testing.TB, opts *ChannelOpts) *tchannel.Channel {
	ch, err := NewClientChannel(opts)
	require.NoError(t, err, "NewServerChannel failed")
	return ch

}
