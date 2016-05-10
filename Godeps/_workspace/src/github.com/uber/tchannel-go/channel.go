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
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/uber/tchannel-go/tnet"

	"golang.org/x/net/context"
)

var (
	errAlreadyListening  = errors.New("channel already listening")
	errInvalidStateForOp = errors.New("channel is in an invalid state for that method")

	// ErrNoServiceName is returned when no service name is provided when
	// creating a new channel.
	ErrNoServiceName = errors.New("no service name provided")
)

const (
	ephemeralHostPort = "0.0.0.0:0"

	// DefaultTraceSampleRate is the default sampling rate for traces.
	DefaultTraceSampleRate = 1.0
)

// TraceReporterFactory is the interface of the method to generate TraceReporter instance.
type TraceReporterFactory func(*Channel) TraceReporter

// ChannelOptions are used to control parameters on a create a TChannel
type ChannelOptions struct {
	// Default Connection options
	DefaultConnectionOptions ConnectionOptions

	// The name of the process, for logging and reporting to peers
	ProcessName string

	// The logger to use for this channel
	Logger Logger

	// The reporter to use for reporting stats for this channel.
	StatsReporter StatsReporter

	// TimeNow is a variable for overriding time.Now in unit tests.
	// Note: This is not a stable part of the API and may change.
	TimeNow func() time.Time

	// Trace reporter to use for this channel.
	TraceReporter TraceReporter

	// Trace reporter factory to generate trace reporter instance.
	TraceReporterFactory TraceReporterFactory

	// TraceSampleRate is the rate of requests to sample, and should be in the range [0, 1].
	// If this value is not set, then DefaultTraceSampleRate is used.
	TraceSampleRate *float64
}

// ChannelState is the state of a channel.
type ChannelState int

const (
	// ChannelClient is a channel that can be used as a client.
	ChannelClient ChannelState = iota + 1

	// ChannelListening is a channel that is listening for new connnections.
	ChannelListening

	// ChannelStartClose is a channel that has received a Close request.
	// The channel is no longer listening, and all new incoming connections are rejected.
	ChannelStartClose

	// ChannelInboundClosed is a channel that has drained all incoming connections, but may
	// have outgoing connections. All incoming calls and new outgoing calls are rejected.
	ChannelInboundClosed

	// ChannelClosed is a channel that has closed completely.
	ChannelClosed
)

//go:generate stringer -type=ChannelState

// A Channel is a bi-directional connection to the peering and routing network.
// Applications can use a Channel to make service calls to remote peers via
// BeginCall, or to listen for incoming calls from peers.  Applications that
// want to receive requests should call one of Serve or ListenAndServe
// TODO(prashant): Shutdown all subchannels + peers when channel is closed.
type Channel struct {
	channelConnectionCommon

	createdStack      string
	commonStatsTags   map[string]string
	connectionOptions ConnectionOptions
	peers             *PeerList

	// mutable contains all the members of Channel which are mutable.
	mutable struct {
		sync.RWMutex // protects members of the mutable struct.
		state        ChannelState
		peerInfo     LocalPeerInfo // May be ephemeral if this is a client only channel
		l            net.Listener  // May be nil if this is a client only channel
		conns        map[uint32]*Connection
	}
}

// channelConnectionCommon is the list of common objects that both use
// and can be copied directly from the channel to the connection.
type channelConnectionCommon struct {
	log             Logger
	statsReporter   StatsReporter
	traceReporter   TraceReporter
	subChannels     *subChannelMap
	timeNow         func() time.Time
	traceSampleRate float64
}

// NewChannel creates a new Channel.  The new channel can be used to send outbound requests
// to peers, but will not listen or handling incoming requests until one of ListenAndServe
// or Serve is called. The local service name should be passed to serviceName.
func NewChannel(serviceName string, opts *ChannelOptions) (*Channel, error) {
	if serviceName == "" {
		return nil, ErrNoServiceName
	}

	if opts == nil {
		opts = &ChannelOptions{}
	}

	logger := opts.Logger
	if logger == nil {
		logger = NullLogger
	}

	processName := opts.ProcessName
	if processName == "" {
		processName = fmt.Sprintf("%s[%d]", filepath.Base(os.Args[0]), os.Getpid())
	}

	statsReporter := opts.StatsReporter
	if statsReporter == nil {
		statsReporter = NullStatsReporter
	}

	timeNow := opts.TimeNow
	if timeNow == nil {
		timeNow = time.Now
	}

	traceSampleRate := DefaultTraceSampleRate
	if opts.TraceSampleRate != nil {
		traceSampleRate = *opts.TraceSampleRate
	}

	ch := &Channel{
		channelConnectionCommon: channelConnectionCommon{
			log: logger.WithFields(
				LogField{"service", serviceName},
				LogField{"process", processName}),
			statsReporter:   statsReporter,
			subChannels:     &subChannelMap{},
			timeNow:         timeNow,
			traceSampleRate: traceSampleRate,
		},

		connectionOptions: opts.DefaultConnectionOptions,
	}
	ch.peers = newRootPeerList(ch).newChild()

	ch.mutable.peerInfo = LocalPeerInfo{
		PeerInfo: PeerInfo{
			ProcessName: processName,
			HostPort:    ephemeralHostPort,
		},
		ServiceName: serviceName,
	}
	ch.mutable.state = ChannelClient
	ch.mutable.conns = make(map[uint32]*Connection)
	ch.createCommonStats()

	// TraceReporter may use the channel, so we must initialize it once the channel is ready.
	traceReporter := opts.TraceReporter
	if opts.TraceReporterFactory != nil {
		traceReporter = opts.TraceReporterFactory(ch)
	}
	if traceReporter == nil {
		traceReporter = NullReporter
	}
	ch.traceReporter = traceReporter

	ch.registerInternal()

	registerNewChannel(ch)
	return ch, nil
}

// ConnectionOptions returns the channel's connection options.
func (ch *Channel) ConnectionOptions() *ConnectionOptions {
	return &ch.connectionOptions
}

// Serve serves incoming requests using the provided listener.
// The local peer info is set synchronously, but the actual socket listening is done in
// a separate goroutine.
func (ch *Channel) Serve(l net.Listener) error {
	mutable := &ch.mutable
	mutable.Lock()
	defer mutable.Unlock()

	if mutable.l != nil {
		return errAlreadyListening
	}
	mutable.l = tnet.Wrap(l)

	if mutable.state != ChannelClient {
		return errInvalidStateForOp
	}
	mutable.state = ChannelListening

	mutable.peerInfo.HostPort = l.Addr().String()
	ch.log = ch.log.WithFields(LogField{"hostPort", mutable.peerInfo.HostPort})

	peerInfo := mutable.peerInfo
	ch.log.Debugf("%v (%v) listening on %v", peerInfo.ProcessName, peerInfo.ServiceName, peerInfo.HostPort)
	go ch.serve()
	return nil
}

// ListenAndServe listens on the given address and serves incoming requests.
// The port may be 0, in which case the channel will use an OS assigned port
// This method does not block as the handling of connections is done in a goroutine.
func (ch *Channel) ListenAndServe(hostPort string) error {
	mutable := &ch.mutable
	mutable.RLock()

	if mutable.l != nil {
		mutable.RUnlock()
		return errAlreadyListening
	}

	l, err := net.Listen("tcp", hostPort)
	if err != nil {
		mutable.RUnlock()
		return err
	}

	mutable.RUnlock()
	return ch.Serve(l)
}

// Registrar is the base interface for registering handlers on either the base
// Channel or the SubChannel
type Registrar interface {
	// ServiceName returns the service name that this Registrar is for.
	ServiceName() string

	// Register registers a handler for ServiceName and the given method.
	Register(h Handler, methodName string)

	// Logger returns the logger for this Registrar.
	Logger() Logger

	// StatsReporter returns the stats reporter for this Registrar
	StatsReporter() StatsReporter

	// StatsTags returns the tags that should be used.
	StatsTags() map[string]string

	// Peers returns the peer list for this Registrar.
	Peers() *PeerList
}

// Register registers a handler for a method.
//
// The handler is registered with the service name used when the Channel was
// created. To register a handler with a different service name, obtain a
// SubChannel for that service with GetSubChannel, and Register a handler
// under that. You may also use SetHandler on a SubChannel to set up a
// catch-all Handler for that service. See the docs for SetHandler for more
// information.
func (ch *Channel) Register(h Handler, methodName string) {
	ch.GetSubChannel(ch.PeerInfo().ServiceName).Register(h, methodName)
}

// PeerInfo returns the current peer info for the channel
func (ch *Channel) PeerInfo() LocalPeerInfo {
	ch.mutable.RLock()
	peerInfo := ch.mutable.peerInfo
	ch.mutable.RUnlock()

	return peerInfo
}

func (ch *Channel) createCommonStats() {
	ch.commonStatsTags = map[string]string{
		"app":     ch.mutable.peerInfo.ProcessName,
		"service": ch.mutable.peerInfo.ServiceName,
	}
	host, err := os.Hostname()
	if err != nil {
		ch.log.Infof("channel failed to get host: %v", err)
		return
	}
	ch.commonStatsTags["host"] = host
	// TODO(prashant): Allow user to pass extra tags (such as cluster, version).
}

// GetSubChannel returns a SubChannel for the given service name. If the subchannel does not
// exist, it is created.
func (ch *Channel) GetSubChannel(serviceName string, opts ...SubChannelOption) *SubChannel {
	sub := ch.subChannels.getOrAdd(serviceName, ch)
	for _, opt := range opts {
		opt(sub)
	}
	return sub
}

// Peers returns the PeerList for the channel.
func (ch *Channel) Peers() *PeerList {
	return ch.peers
}

// rootPeers returns the root PeerList for the channel, which is the sole place
// new Peers are created. All children of the root list (including ch.Peers())
// automatically re-use peers from the root list and create new peers in the
// root list.
func (ch *Channel) rootPeers() *RootPeerList {
	return ch.peers.parent
}

// BeginCall starts a new call to a remote peer, returning an OutboundCall that can
// be used to write the arguments of the call.
func (ch *Channel) BeginCall(ctx context.Context, hostPort, serviceName, methodName string, callOptions *CallOptions) (*OutboundCall, error) {
	p := ch.rootPeers().GetOrAdd(hostPort)
	return p.BeginCall(ctx, serviceName, methodName, callOptions)
}

// serve runs the listener to accept and manage new incoming connections, blocking
// until the channel is closed.
func (ch *Channel) serve() {
	acceptBackoff := 0 * time.Millisecond

	for {
		netConn, err := ch.mutable.l.Accept()
		if err != nil {
			// Backoff from new accepts if this is a temporary error
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if acceptBackoff == 0 {
					acceptBackoff = 5 * time.Millisecond
				} else {
					acceptBackoff *= 2
				}
				if max := 1 * time.Second; acceptBackoff > max {
					acceptBackoff = max
				}
				ch.log.WithFields(
					ErrField(err),
					LogField{"backoff", acceptBackoff},
				).Warn("Accept error, will wait and retry.")
				time.Sleep(acceptBackoff)
				continue
			} else {
				// Only log an error if this didn't happen due to a Close.
				if ch.State() >= ChannelStartClose {
					return
				}
				ch.log.WithFields(ErrField(err)).Fatal("Unrecoverable accept error, closing server.")
				return
			}
		}

		acceptBackoff = 0

		// Register the connection in the peer once the channel is set up.
		events := connectionEvents{
			OnActive:           ch.incomingConnectionActive,
			OnCloseStateChange: ch.connectionCloseStateChange,
			OnExchangeUpdated:  ch.exchangeUpdated,
		}
		if _, err := ch.newInboundConnection(netConn, events); err != nil {
			// Server is getting overloaded - begin rejecting new connections
			ch.log.WithFields(ErrField(err)).Error("Couldn't create new TChannelConnection for incoming conn.")
			netConn.Close()
			continue
		}
	}
}

// Ping sends a ping message to the given hostPort and waits for a response.
func (ch *Channel) Ping(ctx context.Context, hostPort string) error {
	peer := ch.rootPeers().GetOrAdd(hostPort)
	conn, err := peer.GetConnection(ctx)
	if err != nil {
		return err
	}

	return conn.ping(ctx)
}

// Logger returns the logger for this channel.
func (ch *Channel) Logger() Logger {
	return ch.log
}

// StatsReporter returns the stats reporter for this channel.
func (ch *Channel) StatsReporter() StatsReporter {
	return ch.statsReporter
}

// TraceReporter returns the trace reporter for this channel.
func (ch *Channel) TraceReporter() TraceReporter {
	return ch.traceReporter
}

// StatsTags returns the common tags that should be used when reporting stats.
// It returns a new map for each call.
func (ch *Channel) StatsTags() map[string]string {
	m := make(map[string]string)
	for k, v := range ch.commonStatsTags {
		m[k] = v
	}
	return m
}

// ServiceName returns the serviceName that this channel was created for.
func (ch *Channel) ServiceName() string {
	return ch.PeerInfo().ServiceName
}

func getTimeout(ctx context.Context) time.Duration {
	deadline, ok := ctx.Deadline()
	if !ok {
		return DefaultConnectTimeout
	}

	return deadline.Sub(time.Now())
}

// Connect connects the channel.
func (ch *Channel) Connect(ctx context.Context, hostPort string) (*Connection, error) {
	switch state := ch.State(); state {
	case ChannelClient, ChannelListening:
		break
	case ChannelStartClose:
		// We still allow outgoing connections during Close, but the connection has to immediately
		// be Closed after opening
	default:
		ch.log.Debugf("Connect rejecting new connection as state is %v", state)
		return nil, errInvalidStateForOp
	}

	events := connectionEvents{
		OnCloseStateChange: ch.connectionCloseStateChange,
		OnExchangeUpdated:  ch.exchangeUpdated,
	}

	c, err := ch.newOutboundConnection(getTimeout(ctx), hostPort, events)
	if err != nil {
		return nil, err
	}

	if err := c.sendInit(ctx); err != nil {
		return nil, err
	}

	ch.mutable.Lock()
	ch.mutable.conns[c.connID] = c
	chState := ch.mutable.state
	ch.mutable.Unlock()

	// Any connections added after the channel is in StartClose should also be set to start close.
	if chState == ChannelStartClose {
		// TODO(prashant): If Connect is called, but no outgoing calls are made, then this connection
		// will block Close, as it will never get cleaned up.
		c.withStateLock(func() error {
			c.state = connectionStartClose
			return nil
		})
		c.log.Debugf("Channel is in start close, set connection to start close")
	}

	return c, err
}

// exchangeUpdated updates the peer heap.
func (ch *Channel) exchangeUpdated(c *Connection) {
	if c.remotePeerInfo.HostPort == "" {
		// Hostport is unknown until we get init resp.
		return
	}
	p := ch.rootPeers().GetOrAdd(c.remotePeerInfo.HostPort)
	ch.updatePeer(p)
}

// updatePeer updates the score of the peer and update it's position in heap as well.
func (ch *Channel) updatePeer(p *Peer) {
	ch.peers.updatePeer(p)
	ch.subChannels.updatePeer(p)
	p.callOnUpdateComplete()
}

// incomingConnectionActive adds a new active connection to our peer list.
func (ch *Channel) incomingConnectionActive(c *Connection) {
	c.log.Debugf("Add connection as an active peer for %v", c.remotePeerInfo.HostPort)
	// TODO: Alter TChannel spec to allow optionally include the service name
	// when initializing a connection. As-is, we have to keep these peers in
	// rootPeers (which isn't used for outbound calls) because we don't know
	// what services they implement.
	p := ch.rootPeers().GetOrAdd(c.remotePeerInfo.HostPort)
	p.AddInboundConnection(c)
	ch.updatePeer(p)

	ch.mutable.Lock()
	ch.mutable.conns[c.connID] = c
	ch.mutable.Unlock()
}

// removeClosedConn removes a connection if it's closed.
func (ch *Channel) removeClosedConn(c *Connection) {
	if c.readState() != connectionClosed {
		return
	}

	ch.mutable.Lock()
	delete(ch.mutable.conns, c.connID)
	ch.mutable.Unlock()
}

// connectionCloseStateChange is called when a connection's close state changes.
func (ch *Channel) connectionCloseStateChange(c *Connection) {
	ch.removeClosedConn(c)
	if peer, ok := ch.rootPeers().Get(c.remotePeerInfo.HostPort); ok {
		peer.connectionCloseStateChange(c)
		ch.updatePeer(peer)
	}

	chState := ch.State()
	if chState != ChannelStartClose && chState != ChannelInboundClosed {
		return
	}

	ch.mutable.RLock()
	minState := connectionClosed
	for _, c := range ch.mutable.conns {
		if s := c.readState(); s < minState {
			minState = s
		}
	}
	ch.mutable.RUnlock()

	var updateTo ChannelState
	if minState >= connectionClosed {
		updateTo = ChannelClosed
	} else if minState >= connectionInboundClosed && chState == ChannelStartClose {
		updateTo = ChannelInboundClosed
	}

	if updateTo > 0 {
		ch.mutable.Lock()
		ch.mutable.state = updateTo
		ch.mutable.Unlock()
		chState = updateTo
	}

	c.log.Debugf("ConnectionCloseStateChange channel state = %v connection minState = %v",
		chState, minState)
}

// Closed returns whether this channel has been closed with .Close()
func (ch *Channel) Closed() bool {
	return ch.State() == ChannelClosed
}

// State returns the current channel state.
func (ch *Channel) State() ChannelState {
	ch.mutable.RLock()
	state := ch.mutable.state
	ch.mutable.RUnlock()

	return state
}

// Close starts a graceful Close for the channel. This does not happen immediately:
// 1. This call closes the Listener and starts closing connections.
// 2. When all incoming connections are drained, the connection blocks new outgoing calls.
// 3. When all connections are drainged, the channel's state is updated to Closed.
func (ch *Channel) Close() {
	ch.Logger().Infof("Channel.Close called")
	var connections []*Connection
	ch.mutable.Lock()

	if ch.mutable.l != nil {
		ch.mutable.l.Close()
	}

	ch.mutable.state = ChannelStartClose
	if len(ch.mutable.conns) == 0 {
		ch.mutable.state = ChannelClosed
	}
	for _, c := range ch.mutable.conns {
		connections = append(connections, c)
	}
	ch.mutable.Unlock()

	for _, c := range connections {
		c.Close()
	}
	removeClosedChannel(ch)
}
