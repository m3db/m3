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

package tchannel

import (
	"encoding/json"
	"runtime"
	"sort"

	"golang.org/x/net/context"
)

// IntrospectionOptions are the options used when introspecting the Channel.
type IntrospectionOptions struct {
	// IncludeExchanges will include all the IDs in the message exchanges.
	IncludeExchanges bool `json:"includeExchanges"`

	// IncludeEmptyPeers will include peers, even if they have no connections.
	IncludeEmptyPeers bool `json:"includeEmptyPeers"`
}

// RuntimeState is a snapshot of the runtime state for a channel.
type RuntimeState struct {
	// CreatedStack is the stack for how this channel was created.
	CreatedStack string `json:"createdStack"`

	// LocalPeer is the local peer information (service name, host-port, etc).
	LocalPeer LocalPeerInfo `json:"localPeer"`

	// SubChannels contains information about any subchannels.
	SubChannels map[string]SubChannelRuntimeState `json:"subChannels"`

	// RootPeers contains information about all the peers on this channel and their connections.
	RootPeers map[string]PeerRuntimeState `json:"rootPeers"`

	// Peers is the list of shared peers for this channel.
	Peers []SubPeerScore `json:"peers"`

	// NumConnections is the number of connections stored in the channel.
	NumConnections int `json:"numConnections"`

	// OtherChannels is information about any other channels running in this process.
	OtherChannels map[string][]ChannelInfo `json:"otherChannels,omitEmpty"`
}

// GoRuntimeStateOptions are the options used when getting Go runtime state.
type GoRuntimeStateOptions struct {
	// IncludeGoStacks will include all goroutine stacks.
	IncludeGoStacks bool `json:"includeGoStacks"`
}

// ChannelInfo is the state of other channels in the same process.
type ChannelInfo struct {
	CreatedStack string        `json:"createdStack"`
	LocalPeer    LocalPeerInfo `json:"localPeer"`
}

// GoRuntimeState is a snapshot of runtime stats from the runtime.
type GoRuntimeState struct {
	MemStats      runtime.MemStats `json:"memStats"`
	NumGoroutines int              `json:"numGoRoutines"`
	NumCPU        int              `json:"numCPU"`
	NumCGo        int64            `json:"numCGo"`
	GoStacks      []byte           `json:"goStacks,omitempty"`
}

// SubChannelRuntimeState is the runtime state for a subchannel.
type SubChannelRuntimeState struct {
	Service  string `json:"service"`
	Isolated bool   `json:"isolated"`
	// IsolatedPeers is the list of all isolated peers for this channel.
	IsolatedPeers []SubPeerScore      `json:"isolatedPeers,omitempty"`
	Handler       HandlerRuntimeState `json:"handler"`
}

// HandlerRuntimeState TODO
type HandlerRuntimeState struct {
	Type    handlerType `json:"type"`
	Methods []string    `json:"methods,omitempty"`
}

type handlerType string

func (h handlerType) String() string { return string(h) }

const (
	methodHandler   handlerType = "methods"
	overrideHandler             = "overriden"
)

// SubPeerScore show the runtime state of a peer with score.
type SubPeerScore struct {
	HostPort string `json:"hostPort"`
	Score    uint64 `json:"score"`
}

// ConnectionRuntimeState is the runtime state for a single connection.
type ConnectionRuntimeState struct {
	ID               uint32                  `json:"id"`
	ConnectionState  string                  `json:"connectionState"`
	LocalHostPort    string                  `json:"localHostPort"`
	RemoteHostPort   string                  `json:"remoteHostPort"`
	IsEphemeral      bool                    `json:"isEphemeral"`
	InboundExchange  ExchangeSetRuntimeState `json:"inboundExchange"`
	OutboundExchange ExchangeSetRuntimeState `json:"outboundExchange"`
}

// ExchangeSetRuntimeState is the runtime state for a message exchange set.
type ExchangeSetRuntimeState struct {
	Name      string                 `json:"name"`
	Count     int                    `json:"count"`
	Exchanges []ExchangeRuntimeState `json:"exchanges,omitempty"`
}

// ExchangeRuntimeState is the runtime state for a single message exchange.
type ExchangeRuntimeState struct {
	ID          uint32      `json:"id"`
	MessageType messageType `json:"messageType"`
}

// PeerRuntimeState is the runtime state for a single peer.
type PeerRuntimeState struct {
	HostPort            string                   `json:"hostPort"`
	OutboundConnections []ConnectionRuntimeState `json:"outboundConnections"`
	InboundConnections  []ConnectionRuntimeState `json:"inboundConnections"`
	ChosenCount         uint64                   `json:"chosenCount"`
	SCCount             uint32                   `json:"scCount"`
}

// IntrospectState returns the RuntimeState for this channel.
// Note: this is purely for debugging and monitoring, and may slow down your Channel.
func (ch *Channel) IntrospectState(opts *IntrospectionOptions) *RuntimeState {
	if opts == nil {
		opts = &IntrospectionOptions{}
	}

	ch.mutable.RLock()
	conns := len(ch.mutable.conns)
	ch.mutable.RUnlock()

	return &RuntimeState{
		CreatedStack:   ch.createdStack,
		LocalPeer:      ch.PeerInfo(),
		SubChannels:    ch.subChannels.IntrospectState(opts),
		RootPeers:      ch.rootPeers().IntrospectState(opts),
		Peers:          ch.Peers().IntrospectList(opts),
		NumConnections: conns,
		OtherChannels:  ch.IntrospectOthers(opts),
	}
}

// IntrospectOthers returns the ChannelInfo for all other channels in this process.
func (ch *Channel) IntrospectOthers(opts *IntrospectionOptions) map[string][]ChannelInfo {
	channelMap.Lock()
	defer channelMap.Unlock()

	states := make(map[string][]ChannelInfo)
	for svc, channels := range channelMap.existing {
		channelInfos := make([]ChannelInfo, 0, len(channels))
		for _, otherChan := range channels {
			if ch == otherChan {
				continue
			}
			channelInfos = append(channelInfos, otherChan.ReportInfo(opts))
		}
		states[svc] = channelInfos
	}

	return states
}

// ReportInfo returns ChannelInfo for a channel.
func (ch *Channel) ReportInfo(opts *IntrospectionOptions) ChannelInfo {
	return ChannelInfo{
		CreatedStack: ch.createdStack,
		LocalPeer:    ch.PeerInfo(),
	}
}

type containsPeerList interface {
	Copy() map[string]*Peer
}

func fromPeerList(peers containsPeerList, opts *IntrospectionOptions) map[string]PeerRuntimeState {
	m := make(map[string]PeerRuntimeState)
	for _, peer := range peers.Copy() {
		peerState := peer.IntrospectState(opts)
		if len(peerState.InboundConnections)+len(peerState.OutboundConnections) > 0 || opts.IncludeEmptyPeers {
			m[peer.HostPort()] = peerState
		}
	}
	return m
}

// IntrospectState returns the runtime state of the peer list.
func (l *PeerList) IntrospectState(opts *IntrospectionOptions) map[string]PeerRuntimeState {
	return fromPeerList(l, opts)
}

// IntrospectState returns the runtime state of the
func (l *RootPeerList) IntrospectState(opts *IntrospectionOptions) map[string]PeerRuntimeState {
	return fromPeerList(l, opts)
}

// IntrospectState returns the runtime state of the subchannels.
func (subChMap *subChannelMap) IntrospectState(opts *IntrospectionOptions) map[string]SubChannelRuntimeState {
	m := make(map[string]SubChannelRuntimeState)
	subChMap.RLock()
	for k, sc := range subChMap.subchannels {
		state := SubChannelRuntimeState{
			Service:  k,
			Isolated: sc.Isolated(),
		}
		if state.Isolated {
			state.IsolatedPeers = sc.Peers().IntrospectList(opts)
		}
		if hmap, ok := sc.handler.(*handlerMap); ok {
			state.Handler.Type = methodHandler
			methods := make([]string, 0, len(hmap.handlers))
			for k := range hmap.handlers {
				methods = append(methods, k)
			}
			sort.Strings(methods)
			state.Handler.Methods = methods
		} else {
			state.Handler.Type = overrideHandler
		}
		m[k] = state
	}
	subChMap.RUnlock()
	return m
}

func getConnectionRuntimeState(conns []*Connection, opts *IntrospectionOptions) []ConnectionRuntimeState {
	connStates := make([]ConnectionRuntimeState, len(conns))

	for i, conn := range conns {
		connStates[i] = conn.IntrospectState(opts)
	}

	return connStates
}

// IntrospectState returns the runtime state for this peer.
func (p *Peer) IntrospectState(opts *IntrospectionOptions) PeerRuntimeState {
	p.RLock()
	defer p.RUnlock()

	return PeerRuntimeState{
		HostPort:            p.hostPort,
		InboundConnections:  getConnectionRuntimeState(p.inboundConnections, opts),
		OutboundConnections: getConnectionRuntimeState(p.outboundConnections, opts),
		ChosenCount:         p.chosenCount.Load(),
		SCCount:             p.scCount,
	}
}

// IntrospectState returns the runtime state for this connection.
func (c *Connection) IntrospectState(opts *IntrospectionOptions) ConnectionRuntimeState {
	c.stateMut.RLock()
	defer c.stateMut.RUnlock()

	return ConnectionRuntimeState{
		ID:               c.connID,
		ConnectionState:  c.state.String(),
		LocalHostPort:    c.conn.LocalAddr().String(),
		RemoteHostPort:   c.conn.RemoteAddr().String(),
		IsEphemeral:      c.remotePeerInfo.IsEphemeral,
		InboundExchange:  c.inbound.IntrospectState(opts),
		OutboundExchange: c.outbound.IntrospectState(opts),
	}
}

// IntrospectState returns the runtime state for this messsage exchange set.
func (mexset *messageExchangeSet) IntrospectState(opts *IntrospectionOptions) ExchangeSetRuntimeState {
	mexset.RLock()
	setState := ExchangeSetRuntimeState{
		Name:  mexset.name,
		Count: len(mexset.exchanges),
	}

	if opts.IncludeExchanges {
		setState.Exchanges = make([]ExchangeRuntimeState, 0, len(mexset.exchanges))
		for k, v := range mexset.exchanges {
			state := ExchangeRuntimeState{
				ID:          k,
				MessageType: v.msgType,
			}
			setState.Exchanges = append(setState.Exchanges, state)
		}
	}

	mexset.RUnlock()
	return setState
}

func getStacks(all bool) []byte {
	var buf []byte
	for n := 4096; n < 10*1024*1024; n *= 2 {
		buf = make([]byte, n)
		stackLen := runtime.Stack(buf, all)
		if stackLen < n {
			return buf[:stackLen]
		}
	}

	// return the first 10MB of stacks if we have more than 10MB.
	return buf
}
func (ch *Channel) handleIntrospection(arg3 []byte) interface{} {
	var opts IntrospectionOptions
	json.Unmarshal(arg3, &opts)
	return ch.IntrospectState(&opts)
}

// IntrospectList returns the list of peers (hostport, score) in this peer list.
func (l *PeerList) IntrospectList(opts *IntrospectionOptions) []SubPeerScore {
	var peers []SubPeerScore
	l.RLock()
	for _, ps := range l.peerHeap.peerScores {
		peers = append(peers, SubPeerScore{
			HostPort: ps.Peer.hostPort,
			Score:    ps.score,
		})
	}
	l.RUnlock()

	return peers
}

func handleInternalRuntime(arg3 []byte) interface{} {
	var opts GoRuntimeStateOptions
	json.Unmarshal(arg3, &opts)

	state := GoRuntimeState{
		NumGoroutines: runtime.NumGoroutine(),
		NumCPU:        runtime.NumCPU(),
		NumCGo:        runtime.NumCgoCall(),
	}
	runtime.ReadMemStats(&state.MemStats)
	if opts.IncludeGoStacks {
		state.GoStacks = getStacks(true /* all */)
	}

	return state
}

// registerInternal registers the following internal handlers which return runtime state:
//  _gometa_introspect: TChannel internal state.
//  _gometa_runtime: Golang runtime stats.
func (ch *Channel) registerInternal() {
	endpoints := []struct {
		name    string
		handler func([]byte) interface{}
	}{
		{"_gometa_introspect", ch.handleIntrospection},
		{"_gometa_runtime", handleInternalRuntime},
	}

	for _, ep := range endpoints {
		// We need ep in our closure.
		ep := ep
		handler := func(ctx context.Context, call *InboundCall) {
			var arg2, arg3 []byte
			if err := NewArgReader(call.Arg2Reader()).Read(&arg2); err != nil {
				return
			}
			if err := NewArgReader(call.Arg3Reader()).Read(&arg3); err != nil {
				return
			}
			if err := NewArgWriter(call.Response().Arg2Writer()).Write(nil); err != nil {
				return
			}
			NewArgWriter(call.Response().Arg3Writer()).WriteJSON(ep.handler(arg3))
		}
		ch.Register(HandlerFunc(handler), ep.name)
	}
}
