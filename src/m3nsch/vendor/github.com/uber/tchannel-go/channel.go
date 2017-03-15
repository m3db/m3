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

	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"
)

var (
	errAlreadyListening  = errors.New("channel already listening")
	errInvalidStateForOp = errors.New("channel is in an invalid state for that method")

	// ErrNoServiceName is returned when no service name is provided when
	// creating a new channel.
	ErrNoServiceName = errors.New("no service name provided")
)

const ephemeralHostPort = "0.0.0.0:0"

// ChannelOptions are used to control parameters on a create a TChannel
type ChannelOptions struct {
	// Default Connection options
	DefaultConnectionOptions ConnectionOptions

	// The name of the process, for logging and reporting to peers
	ProcessName string

	// The logger to use for this channel
	Logger Logger

	// The host:port selection implementation to use for relaying. This is an
	// unstable API - breaking changes are likely.
	RelayHost RelayHost

	// The list of service names that should be handled locally by this channel.
	// This is an unstable API - breaking changes are likely.
	RelayLocalHandlers []string

	// The maximum allowable timeout for relayed calls (longer timeouts are
	// clamped to this value). Passing zero uses the default of 2m.
	RelayMaxTimeout time.Duration

	// The reporter to use for reporting stats for this channel.
	StatsReporter StatsReporter

	// TimeNow is a variable for overriding time.Now in unit tests.
	// Note: This is not a stable part of the API and may change.
	TimeNow func() time.Time

	// Tracer is an OpenTracing Tracer used to manage distributed tracing spans.
	// If not set, opentracing.GlobalTracer() is used.
	Tracer opentracing.Tracer
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
	relayHost         RelayHost
	relayMaxTimeout   time.Duration

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
	log           Logger
	relayLocal    map[string]struct{}
	statsReporter StatsReporter
	tracer        opentracing.Tracer
	subChannels   *subChannelMap
	timeNow       func() time.Time
}

// Tracer returns the OpenTracing Tracer for this channel. If no tracer was provided
// in the configuration, returns opentracing.GlobalTracer(). Note that this approach
// allows opentracing.GlobalTracer() to be initialized _after_ the channel is created.
func (ccc channelConnectionCommon) Tracer() opentracing.Tracer {
	if ccc.tracer != nil {
		return ccc.tracer
	}
	return opentracing.GlobalTracer()
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

	processName := opts.ProcessName
	if processName == "" {
		processName = fmt.Sprintf("%s[%d]", filepath.Base(os.Args[0]), os.Getpid())
	}

	logger := opts.Logger
	if logger == nil {
		logger = NullLogger
	}
	logger = logger.WithFields(
		LogField{"service", serviceName},
		LogField{"process", processName},
	)

	statsReporter := opts.StatsReporter
	if statsReporter == nil {
		statsReporter = NullStatsReporter
	}

	timeNow := opts.TimeNow
	if timeNow == nil {
		timeNow = time.Now
	}

	ch := &Channel{
		channelConnectionCommon: channelConnectionCommon{
			log:           logger,
			relayLocal:    toStringSet(opts.RelayLocalHandlers),
			statsReporter: statsReporter,
			subChannels:   &subChannelMap{},
			timeNow:       timeNow,
			tracer:        opts.Tracer,
		},

		connectionOptions: opts.DefaultConnectionOptions,
		relayHost:         opts.RelayHost,
		relayMaxTimeout:   validateRelayMaxTimeout(opts.RelayMaxTimeout, logger),
	}
	ch.peers = newRootPeerList(ch).newChild()

	ch.mutable.peerInfo = LocalPeerInfo{
		PeerInfo: PeerInfo{
			ProcessName: processName,
			HostPort:    ephemeralHostPort,
			IsEphemeral: true,
		},
		ServiceName: serviceName,
	}
	ch.mutable.state = ChannelClient
	ch.mutable.conns = make(map[uint32]*Connection)
	ch.createCommonStats()

	ch.registerInternal()

	registerNewChannel(ch)

	if opts.RelayHost != nil {
		opts.RelayHost.SetChannel(ch)
	}
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
	mutable.peerInfo.IsEphemeral = false
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
		ch.log.WithFields(ErrField(err)).Info("Channel creation failed to get host.")
		return
	}
	ch.commonStatsTags["host"] = host
	// TODO(prashant): Allow user to pass extra tags (such as cluster, version).
}

// GetSubChannel returns a SubChannel for the given service name. If the subchannel does not
// exist, it is created.
func (ch *Channel) GetSubChannel(serviceName string, opts ...SubChannelOption) *SubChannel {
	sub, added := ch.subChannels.getOrAdd(serviceName, ch)
	if added {
		for _, opt := range opts {
			opt(sub)
		}
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
			OnActive:           ch.inboundConnectionActive,
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

// Connect creates a new outbound connection to hostPort.
func (ch *Channel) Connect(ctx context.Context, hostPort string) (*Connection, error) {
	switch state := ch.State(); state {
	case ChannelClient, ChannelListening:
		break
	default:
		ch.log.Debugf("Connect rejecting new connection as state is %v", state)
		return nil, errInvalidStateForOp
	}

	// The context timeout applies to the whole call, but users may want a lower
	// connect timeout (e.g. for streams).
	if params := getTChannelParams(ctx); params != nil && params.connectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, params.connectTimeout)
		defer cancel()
	}

	events := connectionEvents{
		OnActive:           ch.outboundConnectionActive,
		OnCloseStateChange: ch.connectionCloseStateChange,
		OnExchangeUpdated:  ch.exchangeUpdated,
	}

	if err := ctx.Err(); err != nil {
		return nil, GetContextError(err)
	}

	c, err := ch.newOutboundConnection(getTimeout(ctx), hostPort, events)
	if err != nil {
		return nil, err
	}

	if err := c.sendInit(ctx); err != nil {
		return nil, err
	}

	// It's possible that the connection we just created responds with a host:port
	// that is not what we tried to connect to. E.g., we may have connected to
	// 127.0.0.1:1234, but the returned host:port may be 10.0.0.1:1234.
	// In this case, the connection won't be added to 127.0.0.1:1234 peer
	// and so future calls to that peer may end up creating new connections. To
	// avoid this issue, and to avoid clients being aware of any TCP relays, we
	// add the connection to the intended peer.
	if hostPort != c.remotePeerInfo.HostPort {
		c.log.Debugf("Outbound connection host:port mismatch, adding to peer %v", c.remotePeerInfo.HostPort)
		ch.addConnectionToPeer(hostPort, c, outbound)
	}

	return c, nil
}

// exchangeUpdated updates the peer heap.
func (ch *Channel) exchangeUpdated(c *Connection) {
	if c.remotePeerInfo.HostPort == "" {
		// Hostport is unknown until we get init resp.
		return
	}

	p, ok := ch.rootPeers().Get(c.remotePeerInfo.HostPort)
	if !ok {
		return
	}

	ch.updatePeer(p)
}

// updatePeer updates the score of the peer and update it's position in heap as well.
func (ch *Channel) updatePeer(p *Peer) {
	ch.peers.updatePeer(p)
	ch.subChannels.updatePeer(p)
	p.callOnUpdateComplete()
}

// addConnection adds the connection to the channel's list of connection
// if the channel is in a valid state to accept this connection. It returns
// whether the connection was added.
func (ch *Channel) addConnection(c *Connection, direction connectionDirection) bool {
	ch.mutable.Lock()
	defer ch.mutable.Unlock()

	if c.readState() != connectionActive {
		return false
	}

	switch state := ch.mutable.state; state {
	case ChannelClient, ChannelListening:
		break
	default:
		return false
	}

	ch.mutable.conns[c.connID] = c
	return true
}

func (ch *Channel) connectionActive(c *Connection, direction connectionDirection) {
	c.log.Debugf("New active %v connection for peer %v", direction, c.remotePeerInfo.HostPort)

	if added := ch.addConnection(c, direction); !added {
		// The channel isn't in a valid state to accept this connection, close the connection.
		c.log.Debugf("Closing connection due to closing channel state")
		c.Close()
		return
	}

	ch.addConnectionToPeer(c.remotePeerInfo.HostPort, c, direction)
}

func (ch *Channel) addConnectionToPeer(hostPort string, c *Connection, direction connectionDirection) {
	p := ch.rootPeers().GetOrAdd(hostPort)
	if err := p.addConnection(c, direction); err != nil {
		c.log.WithFields(
			LogField{"remoteHostPort", c.remotePeerInfo.HostPort},
			LogField{"direction", direction},
			ErrField(err),
		).Warn("Failed to add connection to peer.")
	}

	ch.updatePeer(p)
}

func (ch *Channel) inboundConnectionActive(c *Connection) {
	ch.connectionActive(c, inbound)
}

func (ch *Channel) outboundConnectionActive(c *Connection) {
	ch.connectionActive(c, outbound)
}

// removeClosedConn removes a connection if it's closed.
// Until a connection is fully closed, the channel must keep track of it.
func (ch *Channel) removeClosedConn(c *Connection) {
	if c.readState() != connectionClosed {
		return
	}

	ch.mutable.Lock()
	delete(ch.mutable.conns, c.connID)
	ch.mutable.Unlock()
}

func (ch *Channel) getMinConnectionState() connectionState {
	minState := connectionClosed
	for _, c := range ch.mutable.conns {
		if s := c.readState(); s < minState {
			minState = s
		}
	}
	return minState
}

// connectionCloseStateChange is called when a connection's close state changes.
func (ch *Channel) connectionCloseStateChange(c *Connection) {
	ch.removeClosedConn(c)
	if peer, ok := ch.rootPeers().Get(c.remotePeerInfo.HostPort); ok {
		peer.connectionCloseStateChange(c)
		ch.updatePeer(peer)
	}
	if c.outboundHP != "" && c.outboundHP != c.remotePeerInfo.HostPort {
		// Outbound connections may be in multiple peers.
		if peer, ok := ch.rootPeers().Get(c.outboundHP); ok {
			peer.connectionCloseStateChange(c)
			ch.updatePeer(peer)
		}
	}

	chState := ch.State()
	if chState != ChannelStartClose && chState != ChannelInboundClosed {
		return
	}

	ch.mutable.RLock()
	minState := ch.getMinConnectionState()
	ch.mutable.RUnlock()

	var updateTo ChannelState
	if minState >= connectionClosed {
		updateTo = ChannelClosed
	} else if minState >= connectionInboundClosed && chState == ChannelStartClose {
		updateTo = ChannelInboundClosed
	}

	if updateTo > 0 {
		ch.mutable.Lock()
		// Recheck the state as it's possible another goroutine changed the state
		// from what we expected, and so we might make a stale change.
		if ch.mutable.state == chState {
			ch.mutable.state = updateTo
		}
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
	ch.Logger().Info("Channel.Close called.")
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

// RelayHost returns the channel's RelayHost, if any.
func (ch *Channel) RelayHost() RelayHost {
	return ch.relayHost
}

func toStringSet(ss []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		set[s] = struct{}{}
	}
	return set
}
