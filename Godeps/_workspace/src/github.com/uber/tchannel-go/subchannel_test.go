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

package tchannel_test

import (
	"io/ioutil"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/raw"
	"github.com/uber/tchannel-go/testutils"
	"golang.org/x/net/context"
)

type chanSet struct {
	main     tchannel.Registrar
	sub      tchannel.Registrar
	isolated tchannel.Registrar
}

func withNewSet(t *testing.T, f func(*testing.T, chanSet)) {
	ch := testutils.NewClient(t, nil)
	f(t, chanSet{
		main:     ch,
		sub:      ch.GetSubChannel("hyperbahn"),
		isolated: ch.GetSubChannel("ringpop", tchannel.Isolated),
	})
}

// Assert that two Registrars have references to the same Peer.
func assertHaveSameRef(t *testing.T, r1, r2 tchannel.Registrar) {
	p1, err := r1.Peers().Get(nil)
	assert.NoError(t, err, "First registrar has no peers.")

	p2, err := r2.Peers().Get(nil)
	assert.NoError(t, err, "Second registrar has no peers.")

	assert.True(t, p1 == p2, "Registrars have references to different peers.")
}

func assertNoPeer(t *testing.T, r tchannel.Registrar) {
	_, err := r.Peers().Get(nil)
	assert.Equal(t, err, tchannel.ErrNoPeers)
}

func TestMainAddVisibility(t *testing.T) {
	withNewSet(t, func(t *testing.T, set chanSet) {
		// Adding a peer to the main channel should be reflected in the
		// subchannel, but not the isolated subchannel.
		set.main.Peers().Add("127.0.0.1:3000")
		assertHaveSameRef(t, set.main, set.sub)
		assertNoPeer(t, set.isolated)
	})
}

func TestSubchannelAddVisibility(t *testing.T) {
	withNewSet(t, func(t *testing.T, set chanSet) {
		// Adding a peer to a non-isolated subchannel should be reflected in
		// the main channel but not in isolated siblings.
		set.sub.Peers().Add("127.0.0.1:3000")
		assertHaveSameRef(t, set.main, set.sub)
		assertNoPeer(t, set.isolated)
	})
}

func TestIsolatedAddVisibility(t *testing.T) {
	withNewSet(t, func(t *testing.T, set chanSet) {
		// Adding a peer to an isolated subchannel shouldn't change the main
		// channel or sibling channels.
		set.isolated.Peers().Add("127.0.0.1:3000")

		_, err := set.isolated.Peers().Get(nil)
		assert.NoError(t, err)

		assertNoPeer(t, set.main)
		assertNoPeer(t, set.sub)
	})
}

func TestAddReusesPeers(t *testing.T) {
	withNewSet(t, func(t *testing.T, set chanSet) {
		// Adding to both a channel and an isolated subchannel shouldn't create
		// two separate peers.
		set.main.Peers().Add("127.0.0.1:3000")
		set.isolated.Peers().Add("127.0.0.1:3000")

		assertHaveSameRef(t, set.main, set.sub)
		assertHaveSameRef(t, set.main, set.isolated)
	})
}

func TestSetHandler(t *testing.T) {
	// Generate a Handler that expects only the given methods to be called.
	genHandler := func(methods ...string) tchannel.Handler {
		allowedMethods := make(map[string]struct{}, len(methods))
		for _, m := range methods {
			allowedMethods[m] = struct{}{}
		}

		return tchannel.HandlerFunc(func(ctx context.Context, call *tchannel.InboundCall) {
			method := call.MethodString()
			assert.Contains(t, allowedMethods, method, "unexpected call to %q", method)
			err := raw.WriteResponse(call.Response(), &raw.Res{Arg3: []byte(method)})
			require.NoError(t, err)
		})
	}

	ch := testutils.NewServer(t, testutils.NewOpts().
		AddLogFilter("Couldn't find handler", 1, "serviceName", "svc2", "method", "bar"))

	// Catch-all handler for the main channel that accepts foo, bar, and baz,
	// and a single registered handler for a different subchannel.
	ch.GetSubChannel("svc1").SetHandler(genHandler("foo", "bar", "baz"))
	ch.GetSubChannel("svc2").Register(genHandler("foo"), "foo")
	defer ch.Close()

	client := testutils.NewClient(t, nil)
	client.Peers().Add(ch.PeerInfo().HostPort)
	defer client.Close()

	tests := []struct {
		Service    string
		Method     string
		ShouldFail bool
	}{
		{"svc1", "foo", false},
		{"svc1", "bar", false},
		{"svc1", "baz", false},

		{"svc2", "foo", false},
		{"svc2", "bar", true},
	}

	for _, tt := range tests {
		c := client.GetSubChannel(tt.Service)
		ctx, _ := tchannel.NewContext(time.Second)
		_, data, _, err := raw.CallSC(ctx, c, tt.Method, nil, []byte("irrelevant"))

		if tt.ShouldFail {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tt.Method, string(data))
		}
	}

	st := ch.IntrospectState(nil)
	assert.Equal(t, "overriden", st.SubChannels["svc1"].Handler.Type.String())
	assert.Nil(t, st.SubChannels["svc1"].Handler.Methods)

	assert.Equal(t, "methods", st.SubChannels["svc2"].Handler.Type.String())
	assert.Equal(t, []string{"foo"}, st.SubChannels["svc2"].Handler.Methods)
}

func TestCannotRegisterAfterSetHandler(t *testing.T) {
	ch, err := tchannel.NewChannel("svc", &tchannel.ChannelOptions{
		Logger: tchannel.NewLogger(ioutil.Discard),
	})
	require.NoError(t, err, "NewChannel failed")

	someHandler := tchannel.HandlerFunc(func(ctx context.Context, call *tchannel.InboundCall) {
		panic("unexpected call")
	})

	anotherHandler := tchannel.HandlerFunc(func(ctx context.Context, call *tchannel.InboundCall) {
		panic("unexpected call")
	})

	ch.GetSubChannel("foo").SetHandler(someHandler)

	// Registering against the original service should not panic but
	// registering against the "foo" service should panic.
	assert.NotPanics(t, func() { ch.Register(anotherHandler, "bar") })
	assert.Panics(t, func() { ch.GetSubChannel("foo").Register(anotherHandler, "bar") })
}
