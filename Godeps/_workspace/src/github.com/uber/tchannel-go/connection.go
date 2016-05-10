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
	"io"
	"math"
	"net"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/uber/tchannel-go/atomic"
	"github.com/uber/tchannel-go/typed"

	"golang.org/x/net/context"
)

// PeerInfo contains information about a TChannel peer
type PeerInfo struct {
	// The host and port that can be used to contact the peer, as encoded by net.JoinHostPort
	HostPort string

	// The logical process name for the peer, used for only for logging / debugging
	ProcessName string

	// IsEphemeral returns whether the remote host:port is ephemeral (e.g. not listening).
	IsEphemeral bool
}

func (p PeerInfo) String() string {
	return fmt.Sprintf("%s(%s)", p.HostPort, p.ProcessName)
}

// IsEphemeralHostPort returns if hostPort is the default ephemeral hostPort.
func (p PeerInfo) IsEphemeralHostPort() bool {
	return p.HostPort == "" || p.HostPort == ephemeralHostPort
}

// LocalPeerInfo adds service name to the peer info, only required for the local peer.
type LocalPeerInfo struct {
	PeerInfo

	// ServiceName is the service name for the local peer.
	ServiceName string
}

func (p LocalPeerInfo) String() string {
	return fmt.Sprintf("%v: %v", p.ServiceName, p.PeerInfo)
}

// CurrentProtocolVersion is the current version of the TChannel protocol
// supported by this stack
const CurrentProtocolVersion = 0x02

// DefaultConnectTimeout is the default timeout used by net.Dial, if no timeout
// is specified in the context.
const DefaultConnectTimeout = 5 * time.Second

var (
	// ErrConnectionClosed is returned when a caller performs an method
	// on a closed connection
	ErrConnectionClosed = errors.New("connection is closed")

	// ErrConnectionNotReady is returned when a caller attempts to send a
	// request through a connection which has not yet been initialized
	ErrConnectionNotReady = errors.New("connection is not yet ready")

	// ErrSendBufferFull is returned when a message cannot be sent to the
	// peer because the frame sending buffer has become full.  Typically
	// this indicates that the connection is stuck and writes have become
	// backed up
	ErrSendBufferFull = errors.New("connection send buffer is full, cannot send frame")

	errConnectionAlreadyActive     = errors.New("connection is already active")
	errConnectionWaitingOnPeerInit = errors.New("connection is waiting for the peer to sent init")
	errCannotHandleInitRes         = errors.New("could not return init-res to handshake thread")
)

// errConnectionInvalidState is returned when the connection is in an unknown state.
type errConnectionUnknownState struct {
	site  string
	state connectionState
}

func (e errConnectionUnknownState) Error() string {
	return fmt.Sprintf("connection is in unknown state: %v at %v", e.state, e.site)
}

// ConnectionOptions are options that control the behavior of a Connection
type ConnectionOptions struct {
	// The frame pool, allowing better management of frame buffers.  Defaults to using raw heap
	FramePool FramePool

	// The size of receive channel buffers.  Defaults to 512
	RecvBufferSize int

	// The size of send channel buffers.  Defaults to 512
	SendBufferSize int

	// The type of checksum to use when sending messages
	ChecksumType ChecksumType
}

// connectionEvents are the events that can be triggered by a connection.
type connectionEvents struct {
	// OnActive is called when a connection becomes active.
	OnActive func(c *Connection)

	// OnCloseStateChange is called when a connection that is closing changes state.
	OnCloseStateChange func(c *Connection)

	// OnExchangeUpdated is called when a message exchange added or removed.
	OnExchangeUpdated func(c *Connection)
}

// Connection represents a connection to a remote peer.
type Connection struct {
	channelConnectionCommon

	connID          uint32
	checksumType    ChecksumType
	framePool       FramePool
	conn            net.Conn
	localPeerInfo   LocalPeerInfo
	remotePeerInfo  PeerInfo
	sendCh          chan *Frame
	state           connectionState
	stateMut        sync.RWMutex
	inbound         *messageExchangeSet
	outbound        *messageExchangeSet
	handler         Handler
	nextMessageID   atomic.Uint32
	events          connectionEvents
	commonStatsTags map[string]string

	// closeNetworkCalled is used to avoid errors from being logged
	// when this side closes a connection.
	closeNetworkCalled atomic.Int32
	// stoppedExchanges is atomically set when exchanges are stopped due to error.
	stoppedExchanges atomic.Uint32
	// pendingMethods is the number of methods running that may block closing of sendCh.
	pendingMethods atomic.Int64
	// ignoreRemotePeer is used to avoid a data race between setting the RemotePeerInfo
	// and the connection failing, causing a read of the RemotePeerInfo at the same time.
	ignoreRemotePeer bool
}

// nextConnID gives an ID for each connection for debugging purposes.
var nextConnID atomic.Uint32

type connectionState int

const (
	// Connection initiated by peer is waiting to recv init-req from peer
	connectionWaitingToRecvInitReq connectionState = iota + 1

	// Connection initated by current process is waiting to send init-req to peer
	connectionWaitingToSendInitReq

	// Connection initiated by current process has sent init-req, and is
	// waiting for init-req
	connectionWaitingToRecvInitRes

	// Connection is fully active
	connectionActive

	// Connection is starting to close; new incoming requests are rejected, outbound
	// requests are allowed to proceed
	connectionStartClose

	// Connection has finished processing all active inbound, and is
	// waiting for outbound requests to complete or timeout
	connectionInboundClosed

	// Connection is fully closed
	connectionClosed
)

//go:generate stringer -type=connectionState

// Creates a new Connection around an outbound connection initiated to a peer
func (ch *Channel) newOutboundConnection(timeout time.Duration, hostPort string, events connectionEvents) (*Connection, error) {
	conn, err := net.DialTimeout("tcp", hostPort, timeout)
	if err != nil {
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			ch.log.WithFields(LogField{"hostPort", hostPort}, LogField{"timeout", timeout}).Infof("Outbound net.Dial timed out")
			err = ErrTimeout
		}
		return nil, err
	}

	return ch.newConnection(conn, connectionWaitingToSendInitReq, events), nil
}

// Creates a new Connection based on an incoming connection from a peer
func (ch *Channel) newInboundConnection(conn net.Conn, events connectionEvents) (*Connection, error) {
	return ch.newConnection(conn, connectionWaitingToRecvInitReq, events), nil
}

// Creates a new connection in a given initial state
func (ch *Channel) newConnection(conn net.Conn, initialState connectionState, events connectionEvents) *Connection {
	opts := &ch.connectionOptions

	checksumType := opts.ChecksumType
	if checksumType == ChecksumTypeNone {
		checksumType = ChecksumTypeCrc32C
	}

	sendBufferSize := opts.SendBufferSize
	if sendBufferSize <= 0 {
		sendBufferSize = 512
	}

	recvBufferSize := opts.RecvBufferSize
	if recvBufferSize <= 0 {
		recvBufferSize = 512
	}

	framePool := opts.FramePool
	if framePool == nil {
		framePool = DefaultFramePool
	}

	connID := nextConnID.Inc()
	log := ch.log.WithFields(LogFields{
		{"connID", connID},
		{"localAddr", conn.LocalAddr()},
		{"remoteAddr", conn.RemoteAddr()},
	}...)
	peerInfo := ch.PeerInfo()
	log.Debugf("created for %v (%v) local: %v remote: %v",
		peerInfo.ServiceName, peerInfo.ProcessName, conn.LocalAddr(), conn.RemoteAddr())
	c := &Connection{
		channelConnectionCommon: ch.channelConnectionCommon,

		connID:          connID,
		conn:            conn,
		framePool:       framePool,
		state:           initialState,
		sendCh:          make(chan *Frame, sendBufferSize),
		localPeerInfo:   peerInfo,
		checksumType:    checksumType,
		inbound:         newMessageExchangeSet(log, messageExchangeSetInbound),
		outbound:        newMessageExchangeSet(log, messageExchangeSetOutbound),
		handler:         channelHandler{ch},
		events:          events,
		commonStatsTags: ch.commonStatsTags,
	}
	c.log = log
	c.inbound.onRemoved = c.checkExchanges
	c.outbound.onRemoved = c.checkExchanges
	c.inbound.onAdded = c.onExchangeAdded
	c.outbound.onAdded = c.onExchangeAdded

	go c.readFrames(connID)
	go c.writeFrames(connID)
	return c
}

func (c *Connection) onExchangeAdded() {
	c.callOnExchangeChange()
}

// IsActive returns whether this connection is in an active state.
func (c *Connection) IsActive() bool {
	return c.readState() == connectionActive
}

func (c *Connection) callOnActive() {
	if f := c.events.OnActive; f != nil {
		f(c)
	}
}

func (c *Connection) callOnCloseStateChange() {
	if f := c.events.OnCloseStateChange; f != nil {
		f(c)
	}
}

func (c *Connection) getInitParams() initParams {
	return initParams{
		InitParamHostPort:                c.localPeerInfo.HostPort,
		InitParamProcessName:             c.localPeerInfo.ProcessName,
		InitParamTChannelLanguage:        "go",
		InitParamTChannelLanguageVersion: strings.TrimPrefix(runtime.Version(), "go"),
		InitParamTChannelVersion:         VersionInfo,
	}
}

func (c *Connection) callOnExchangeChange() {
	if f := c.events.OnExchangeUpdated; f != nil {
		f(c)
	}
}

// Initiates a handshake with a peer.
func (c *Connection) sendInit(ctx context.Context) error {
	err := c.withStateLock(func() error {
		switch c.state {
		case connectionWaitingToSendInitReq:
			c.state = connectionWaitingToRecvInitRes
			return nil
		case connectionWaitingToRecvInitReq:
			return errConnectionWaitingOnPeerInit
		case connectionClosed, connectionStartClose, connectionInboundClosed:
			return ErrConnectionClosed
		case connectionActive, connectionWaitingToRecvInitRes:
			return errConnectionAlreadyActive
		default:
			return errConnectionUnknownState{"sendInit", c.state}
		}
	})
	if err != nil {
		return err
	}

	initMsgID := c.NextMessageID()
	req := initReq{initMessage{id: initMsgID}}
	req.Version = CurrentProtocolVersion
	req.initParams = c.getInitParams()

	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return ErrInvalidConnectionState
	}
	defer c.pendingExchangeMethodDone()

	mex, err := c.outbound.newExchange(ctx, c.framePool, req.messageType(), req.ID(), 1)
	if err != nil {
		return c.connectionError("create init req", err)
	}

	defer c.outbound.removeExchange(req.ID())

	if err := c.sendMessage(&req); err != nil {
		return c.connectionError("send init req", err)
	}

	res := initRes{}
	err = c.recvMessage(ctx, &res, mex)
	if err != nil {
		return c.connectionError("receive init res", err)
	}

	return nil
}

// Handles an incoming InitReq.  If we are waiting for the peer to send us an
// InitReq, and the InitReq is valid, send a corresponding InitRes and mark
// ourselves as active
func (c *Connection) handleInitReq(frame *Frame) {
	id := frame.Header.ID
	var req initReq
	rbuf := typed.NewReadBuffer(frame.SizedPayload())
	if err := req.read(rbuf); err != nil {
		// TODO(mmihic): Technically probably a protocol error
		c.connectionError("parse init req", err)
		return
	}

	if req.Version < CurrentProtocolVersion {
		c.protocolError(id, fmt.Errorf("Unsupported protocol version %d from peer", req.Version))
		return
	}

	var ok bool
	if c.remotePeerInfo.HostPort, ok = req.initParams[InitParamHostPort]; !ok {
		c.protocolError(id, fmt.Errorf("Header %v is required", InitParamHostPort))
		return
	}
	if c.remotePeerInfo.ProcessName, ok = req.initParams[InitParamProcessName]; !ok {
		c.protocolError(id, fmt.Errorf("Header %v is required", InitParamProcessName))
		return
	}
	if c.remotePeerInfo.IsEphemeralHostPort() {
		c.remotePeerInfo.HostPort = c.conn.RemoteAddr().String()
		c.remotePeerInfo.IsEphemeral = true
	}

	res := initRes{initMessage{id: frame.Header.ID}}
	res.initParams = c.getInitParams()
	res.Version = CurrentProtocolVersion
	if err := c.sendMessage(&res); err != nil {
		c.connectionError("send init res", err)
		return
	}

	c.withStateLock(func() error {
		switch c.state {
		case connectionWaitingToRecvInitReq:
			c.state = connectionActive
		}

		return nil
	})

	c.callOnActive()
}

// ping sends a ping message and waits for a ping response.
func (c *Connection) ping(ctx context.Context) error {
	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return ErrInvalidConnectionState
	}
	defer c.pendingExchangeMethodDone()

	req := &pingReq{id: c.NextMessageID()}
	mex, err := c.outbound.newExchange(ctx, c.framePool, req.messageType(), req.ID(), 1)
	if err != nil {
		return c.connectionError("create ping exchange", err)
	}
	defer c.outbound.removeExchange(req.ID())

	if err := c.sendMessage(req); err != nil {
		return c.connectionError("send ping", err)
	}

	res := &pingRes{}
	err = c.recvMessage(ctx, res, mex)
	if err != nil {
		return c.connectionError("receive pong", err)
	}

	return nil
}

// handlePingRes calls registered ping handlers.
func (c *Connection) handlePingRes(frame *Frame) bool {
	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		c.log.WithFields(LogField{"response", frame.Header}).Warn("Unexpected ping response.")
		return true
	}
	// ping req is waiting for this frame, and will release it.
	return false
}

// handlePingReq responds to the pingReq message with a pingRes.
func (c *Connection) handlePingReq(frame *Frame) {
	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return
	}
	defer c.pendingExchangeMethodDone()

	if c.readState() != connectionActive {
		c.protocolError(frame.Header.ID, fmt.Errorf("connection state is not active"))
		return
	}

	pingRes := &pingRes{id: frame.Header.ID}
	if err := c.sendMessage(pingRes); err != nil {
		c.connectionError("send pong", err)
	}
}

// Handles an incoming InitRes.  If we are waiting for the peer to send us an
// InitRes, forward the InitRes to the waiting goroutine
func (c *Connection) handleInitRes(frame *Frame) bool {
	var err error
	switch state := c.readState(); state {
	case connectionWaitingToRecvInitRes:
		err = nil
	case connectionClosed, connectionStartClose, connectionInboundClosed:
		err = ErrConnectionClosed
	case connectionActive:
		err = errConnectionAlreadyActive
	case connectionWaitingToSendInitReq:
		err = ErrConnectionNotReady
	case connectionWaitingToRecvInitReq:
		err = errConnectionWaitingOnPeerInit
	default:
		err = errConnectionUnknownState{"handleInitRes", state}
	}
	if err != nil {
		c.connectionError("handle init res", err)
		return true
	}

	res := initRes{initMessage{id: frame.Header.ID}}
	if err := frame.read(&res); err != nil {
		c.connectionError("parse init res", fmt.Errorf("failed to read initRes from frame"))
		return true
	}

	if res.Version != CurrentProtocolVersion {
		c.protocolError(frame.Header.ID, fmt.Errorf("unsupported protocol version %d from peer", res.Version))
		return true
	}

	c.withStateLock(func() error {
		if c.state != connectionWaitingToRecvInitRes {
			return nil
		}
		if c.ignoreRemotePeer {
			return nil
		}

		c.remotePeerInfo.HostPort = res.initParams[InitParamHostPort]
		if c.remotePeerInfo.IsEphemeralHostPort() {
			c.remotePeerInfo.HostPort = c.conn.RemoteAddr().String()
			c.remotePeerInfo.IsEphemeral = true
		}
		c.remotePeerInfo.ProcessName = res.initParams[InitParamProcessName]

		c.state = connectionActive
		return nil
	})
	c.callOnActive()

	// We forward the peer frame, as the other side is blocked waiting on this frame.
	// Rather than add another mechanism, we use the mex to block the sender till we get initRes.
	if err := c.outbound.forwardPeerFrame(frame); err != nil {
		c.connectionError("forard init res", errCannotHandleInitRes)
		return true
	}

	// init req waits for this message and will release it when done.
	return false
}

// sendMessage sends a standalone message (typically a control message)
func (c *Connection) sendMessage(msg message) error {
	frame := c.framePool.Get()
	if err := frame.write(msg); err != nil {
		c.framePool.Release(frame)
		return err
	}

	select {
	case c.sendCh <- frame:
		return nil
	default:
		return ErrSendBufferFull
	}
}

// recvMessage blocks waiting for a standalone response message (typically a
// control message)
func (c *Connection) recvMessage(ctx context.Context, msg message, mex *messageExchange) error {
	frame, err := mex.recvPeerFrameOfType(msg.messageType())
	if err != nil {
		if err, ok := err.(errorMessage); ok {
			return err.AsSystemError()
		}
		return err
	}

	err = frame.read(msg)
	c.framePool.Release(frame)
	return err
}

// RemotePeerInfo returns the peer info for the remote peer.
func (c *Connection) RemotePeerInfo() PeerInfo {
	return c.remotePeerInfo
}

// NextMessageID reserves the next available message id for this connection
func (c *Connection) NextMessageID() uint32 {
	return c.nextMessageID.Inc()
}

// SendSystemError sends an error frame for the given system error.
func (c *Connection) SendSystemError(id uint32, span *Span, err error) error {
	frame := c.framePool.Get()

	errorSpan := Span{}
	if span != nil {
		errorSpan = *span
	}

	if err := frame.write(&errorMessage{
		id:      id,
		errCode: GetSystemErrorCode(err),
		tracing: errorSpan,
		message: GetSystemErrorMessage(err),
	}); err != nil {

		// This shouldn't happen - it means writing the errorMessage is broken.
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			LogField{"id", id},
			ErrField(err),
		).Warn("Couldn't create outbound frame.")
		return fmt.Errorf("failed to create outbound error frame")
	}

	// When sending errors, we hold the state rlock to ensure that sendCh is not closed
	// as we are sending the frame.
	return c.withStateRLock(func() error {
		// Errors cannot be sent if the connection has been closed.
		if c.state == connectionClosed {
			c.log.Infof("Could not send error frame to %s on closed conn for %d : %v",
				c.remotePeerInfo, id, err)
			return fmt.Errorf("failed to send error frame, connection state %v", c.state)
		}

		select {
		case c.sendCh <- frame: // Good to go
			return nil
		default: // If the send buffer is full, log and return an error.
		}
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			LogField{"id", id},
			ErrField(err),
		).Warn("Couldn't send outbound frame.")
		return fmt.Errorf("failed to send error frame, buffer full")
	})
}

func (c *Connection) logConnectionError(site string, err error) error {
	errCode := ErrCodeNetwork
	if err == io.EOF {
		c.log.Debugf("Connection got EOF")
	} else {
		logger := c.log.WithFields(
			LogField{"site", site},
			ErrField(err),
		)
		if se, ok := err.(SystemError); ok && se.Code() != ErrCodeNetwork {
			errCode = se.Code()
			logger.Error("Connection error.")
		} else {
			logger.Warn("Connection error.")
		}
	}
	return NewWrappedSystemError(errCode, err)
}

// connectionError handles a connection level error
func (c *Connection) connectionError(site string, err error) error {
	// Avoid racing with setting the peer info.
	c.withStateLock(func() error {
		c.ignoreRemotePeer = true
		return nil
	})

	err = c.logConnectionError(site, err)
	c.Close()

	// On any connection error, notify the exchanges of this error.
	if c.stoppedExchanges.CAS(0, 1) {
		c.outbound.stopExchanges(err)
		c.inbound.stopExchanges(err)
	}
	return err
}

func (c *Connection) protocolError(id uint32, err error) error {
	c.log.WithFields(ErrField(err)).Warn("Protocol error.")
	sysErr := NewWrappedSystemError(ErrCodeProtocol, err)
	c.SendSystemError(id, nil, sysErr)
	// Don't close the connection until the error has been sent.
	c.Close()
	return sysErr
}

// withStateLock performs an action with the connection state mutex locked
func (c *Connection) withStateLock(f func() error) error {
	c.stateMut.Lock()
	err := f()
	c.stateMut.Unlock()

	return err
}

// withStateRLock performs an action with the connection state mutex rlocked.
func (c *Connection) withStateRLock(f func() error) error {
	c.stateMut.RLock()
	err := f()
	c.stateMut.RUnlock()

	return err
}

func (c *Connection) readState() connectionState {
	c.stateMut.RLock()
	state := c.state
	c.stateMut.RUnlock()
	return state
}

// readFrames is the loop that reads frames from the network connection and
// dispatches to the appropriate handler. Run within its own goroutine to
// prevent overlapping reads on the socket.  Most handlers simply send the
// incoming frame to a channel; the init handlers are a notable exception,
// since we cannot process new frames until the initialization is complete.
func (c *Connection) readFrames(_ uint32) {
	for {
		frame := c.framePool.Get()
		if err := frame.ReadIn(c.conn); err != nil {
			if c.closeNetworkCalled.Load() == 0 {
				c.connectionError("read frames", err)
			} else {
				c.log.Debugf("Ignoring error after connection was closed: %v", err)
			}
			c.framePool.Release(frame)
			return
		}

		// call req and call res messages may not want the frame released immediately.
		releaseFrame := true
		switch frame.Header.messageType {
		case messageTypeCallReq:
			releaseFrame = c.handleCallReq(frame)
		case messageTypeCallReqContinue:
			releaseFrame = c.handleCallReqContinue(frame)
		case messageTypeCallRes:
			releaseFrame = c.handleCallRes(frame)
		case messageTypeCallResContinue:
			releaseFrame = c.handleCallResContinue(frame)
		case messageTypeInitReq:
			c.handleInitReq(frame)
		case messageTypeInitRes:
			releaseFrame = c.handleInitRes(frame)
		case messageTypePingReq:
			c.handlePingReq(frame)
		case messageTypePingRes:
			releaseFrame = c.handlePingRes(frame)
		case messageTypeError:
			releaseFrame = c.handleError(frame)
		default:
			// TODO(mmihic): Log and close connection with protocol error
			c.log.WithFields(
				LogField{"header", frame.Header},
				LogField{"remotePeer", c.remotePeerInfo},
			).Error("Received unexpected frame.")
		}

		if releaseFrame {
			c.framePool.Release(frame)
		}
	}
}

// writeFrames is the main loop that pulls frames from the send channel and
// writes them to the connection.
func (c *Connection) writeFrames(_ uint32) {
	for f := range c.sendCh {
		if c.log.Enabled(LogLevelDebug) {
			c.log.Debugf("Writing frame %s", f.Header)
		}

		err := f.WriteOut(c.conn)
		c.framePool.Release(f)
		if err != nil {
			c.connectionError("write frames", err)
			return
		}
	}

	// Close the network after we have sent the last frame
	c.closeNetwork()
}

// pendingExchangeMethodAdd returns whether the method that is trying to
// add a message exchange can continue.
func (c *Connection) pendingExchangeMethodAdd() bool {
	return c.pendingMethods.Inc() > 0
}

// pendingExchangeMethodDone should be deferred by a method called
// pendingExchangeMessageAdd.
func (c *Connection) pendingExchangeMethodDone() {
	c.pendingMethods.Dec()
}

// closeSendCh waits till there are no other goroutines that may try to write
// to sendCh.
// We accept connID on the stack so can more easily debug panics or leaked goroutines.
func (c *Connection) closeSendCh(connID uint32) {
	// Wait till all methods that may add exchanges are done running.
	// When they are done, we set the value to a negative value which
	// will ensure that if any other methods start that may add exchanges
	// they will fail due to closed connection.
	for !c.pendingMethods.CAS(0, math.MinInt32) {
		time.Sleep(time.Millisecond)
	}

	c.inbound.waitForSendCh()
	c.outbound.waitForSendCh()
	c.log.Debugf("Closing send channel.")
	close(c.sendCh)
}

// checkExchanges is called whenever an exchange is removed, and when Close is called.
func (c *Connection) checkExchanges() {
	c.callOnExchangeChange()
	moveState := func(fromState, toState connectionState) bool {
		err := c.withStateLock(func() error {
			if c.state != fromState {
				return errors.New("")
			}
			c.state = toState
			return nil
		})
		return err == nil
	}

	var updated connectionState
	if c.readState() == connectionStartClose {
		if c.inbound.count() == 0 && moveState(connectionStartClose, connectionInboundClosed) {
			updated = connectionInboundClosed
		}
		// If there was no update to the state, there's no more processing to do.
		if updated == 0 {
			return
		}
	}

	if c.readState() == connectionInboundClosed {
		if c.outbound.count() == 0 && moveState(connectionInboundClosed, connectionClosed) {
			updated = connectionClosed
		}
	}

	if updated != 0 {
		// If the connection is closed, we can safely close the channel.
		if updated == connectionClosed {
			go c.closeSendCh(c.connID)
		}

		c.log.Debugf("checkExchanges updated connection state to %v", updated)
		c.callOnCloseStateChange()
	}
}

// Close starts a graceful Close which will first reject incoming calls, reject outgoing calls
// before finally marking the connection state as closed.
func (c *Connection) Close() error {
	c.log.Debugf("Connection Close")

	var closeSendCh bool
	// Update the state which will start blocking incoming calls.
	if err := c.withStateLock(func() error {
		switch c.state {
		case connectionActive:
			c.state = connectionStartClose
		case connectionWaitingToRecvInitReq, connectionWaitingToRecvInitRes:
			// If the connection isn't active yet, it can be closed after messages in sendCh.
			c.state = connectionClosed
			closeSendCh = true
		default:
			return fmt.Errorf("connection must be Active to Close")
		}
		return nil
	}); err != nil {
		return err
	}

	if closeSendCh {
		go c.closeSendCh(c.connID)
	}

	// Check all in-flight requests to see whether we can transition the Close state.
	c.checkExchanges()

	return nil
}

// closeNetwork closes the network connection and all network-related channels.
// This should only be done in response to a fatal connection or protocol
// error, or after all pending frames have been sent.
func (c *Connection) closeNetwork() {
	// NB(mmihic): The sender goroutine will exit once the connection is
	// closed; no need to close the send channel (and closing the send
	// channel would be dangerous since other goroutine might be sending)
	c.log.Debugf("Closing underlying network connection")
	c.closeNetworkCalled.Inc()
	if err := c.conn.Close(); err != nil {
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			ErrField(err),
		).Warn("Couldn't close connection to peer.")
	}
}
