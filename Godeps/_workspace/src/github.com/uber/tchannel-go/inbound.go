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
	"time"

	"golang.org/x/net/context"
)

var errInboundRequestAlreadyActive = errors.New("inbound request is already active; possible duplicate client id")

// handleCallReq handles an incoming call request, registering a message
// exchange to receive further fragments for that call, and dispatching it in
// another goroutine
func (c *Connection) handleCallReq(frame *Frame) bool {
	now := c.timeNow()
	switch state := c.readState(); state {
	case connectionActive:
		break
	case connectionStartClose, connectionInboundClosed, connectionClosed:
		c.SendSystemError(frame.Header.ID, nil, ErrChannelClosed)
		return true
	case connectionWaitingToRecvInitReq, connectionWaitingToSendInitReq, connectionWaitingToRecvInitRes:
		c.SendSystemError(frame.Header.ID, nil, NewSystemError(ErrCodeDeclined, "connection not ready"))
		return true
	default:
		panic(fmt.Errorf("unknown connection state for call req: %v", state))
	}

	callReq := new(callReq)
	initialFragment, err := parseInboundFragment(c.framePool, frame, callReq)
	if err != nil {
		// TODO(mmihic): Probably want to treat this as a protocol error
		c.log.WithFields(
			LogField{"header", frame.Header},
			ErrField(err),
		).Error("Couldn't decode initial fragment.")
		return true
	}

	call := new(InboundCall)
	call.conn = c
	ctx, cancel := newIncomingContext(call, callReq.TimeToLive, &callReq.Tracing)

	if !c.pendingExchangeMethodAdd() {
		// Connection is closed, no need to do anything.
		return true
	}
	defer c.pendingExchangeMethodDone()

	mex, err := c.inbound.newExchange(ctx, c.framePool, callReq.messageType(), frame.Header.ID, mexChannelBufferSize)
	if err != nil {
		if err == errDuplicateMex {
			err = errInboundRequestAlreadyActive
		}
		c.log.WithFields(LogField{"header", frame.Header}).Error("Couldn't register exchange.")
		c.protocolError(frame.Header.ID, errInboundRequestAlreadyActive)
		return true
	}

	// Close may have been called between the time we checked the state and us creating the exchange.
	if c.readState() != connectionActive {
		mex.shutdown()
		return true
	}

	response := new(InboundCallResponse)
	response.call = call
	response.calledAt = now
	response.Annotations = Annotations{
		reporter: c.traceReporter,
		timeNow:  c.timeNow,
		data: TraceData{
			Span: callReq.Tracing,
			Source: TraceEndpoint{
				HostPort:    c.localPeerInfo.HostPort,
				ServiceName: callReq.Service,
			},
		},
		binaryAnnotationsBacking: [2]BinaryAnnotation{
			{Key: "cn", Value: callReq.Headers[CallerName]},
			{Key: "as", Value: callReq.Headers[ArgScheme]},
		},
	}
	response.data.Target = response.data.Source
	response.data.Annotations = response.annotationsBacking[:0]
	response.data.BinaryAnnotations = response.binaryAnnotationsBacking[:]
	response.AddAnnotationAt(AnnotationKeyServerReceive, now)
	response.mex = mex
	response.conn = c
	response.cancel = cancel
	response.span = callReq.Tracing
	response.log = c.log.WithFields(LogField{"In-Response", callReq.ID()})
	response.contents = newFragmentingWriter(response.log, response, initialFragment.checksumType.New())
	response.headers = transportHeaders{}
	response.messageForFragment = func(initial bool) message {
		if initial {
			callRes := new(callRes)
			callRes.Headers = response.headers
			callRes.ResponseCode = responseOK
			if response.applicationError {
				callRes.ResponseCode = responseApplicationError
			}
			return callRes
		}

		return new(callResContinue)
	}

	call.mex = mex
	call.initialFragment = initialFragment
	call.serviceName = string(callReq.Service)
	call.headers = callReq.Headers
	call.span = callReq.Tracing
	call.response = response
	call.log = c.log.WithFields(LogField{"In-Call", callReq.ID()})
	call.messageForFragment = func(initial bool) message { return new(callReqContinue) }
	call.contents = newFragmentingReader(call.log, call)
	call.statsReporter = c.statsReporter
	call.createStatsTags(c.commonStatsTags)

	response.statsReporter = c.statsReporter
	response.commonStatsTags = call.commonStatsTags

	setResponseHeaders(call.headers, response.headers)
	go c.dispatchInbound(c.connID, callReq.ID(), call, frame)
	return false
}

// handleCallReqContinue handles the continuation of a call request, forwarding
// it to the request channel for that request, where it can be pulled during
// defragmentation
func (c *Connection) handleCallReqContinue(frame *Frame) bool {
	if err := c.inbound.forwardPeerFrame(frame); err != nil {
		// If forward fails, it's due to a timeout. We can free this frame.
		return true
	}
	return false
}

// createStatsTags creates the common stats tags, if they are not already created.
func (call *InboundCall) createStatsTags(connectionTags map[string]string) {
	call.commonStatsTags = map[string]string{
		"calling-service": call.CallerName(),
	}
	for k, v := range connectionTags {
		call.commonStatsTags[k] = v
	}
}

// dispatchInbound ispatches an inbound call to the appropriate handler
func (c *Connection) dispatchInbound(_ uint32, _ uint32, call *InboundCall, frame *Frame) {
	if c.log.Enabled(LogLevelDebug) {
		c.log.Debugf("Received incoming call for %s from %s", call.ServiceName(), c.remotePeerInfo)
	}

	if err := call.readMethod(); err != nil {
		c.log.WithFields(
			LogField{"remotePeer", c.remotePeerInfo},
			ErrField(err),
		).Error("Couldn't read method.")
		c.framePool.Release(frame)
		return
	}

	call.commonStatsTags["endpoint"] = string(call.method)
	call.statsReporter.IncCounter("inbound.calls.recvd", call.commonStatsTags, 1)
	call.response.SetMethod(string(call.method))

	// TODO(prashant): This is an expensive way to check for cancellation. Use a heap for timeouts.
	go func() {
		select {
		case <-call.mex.ctx.Done():
			if call.mex.ctx.Err() == context.DeadlineExceeded {
				call.mex.inboundTimeout()
			}
		case <-call.mex.errCh.c:
			if c.log.Enabled(LogLevelDebug) {
				c.log.Debugf("Wait for timeout/cancellation interrupted by error: %v", call.mex.errCh.err)
			}
		}
	}()

	c.handler.Handle(call.mex.ctx, call)
}

// An InboundCall is an incoming call from a peer
type InboundCall struct {
	reqResReader

	conn            *Connection
	response        *InboundCallResponse
	serviceName     string
	method          []byte
	methodString    string
	headers         transportHeaders
	span            Span
	statsReporter   StatsReporter
	commonStatsTags map[string]string
}

// ServiceName returns the name of the service being called
func (call *InboundCall) ServiceName() string {
	return call.serviceName
}

// Method returns the method being called
func (call *InboundCall) Method() []byte {
	return call.method
}

// MethodString returns the method being called as a string.
func (call *InboundCall) MethodString() string {
	return call.methodString
}

// Format the format of the request from the ArgScheme transport header.
func (call *InboundCall) Format() Format {
	return Format(call.headers[ArgScheme])
}

// CallerName returns the caller name from the CallerName transport header.
func (call *InboundCall) CallerName() string {
	return call.headers[CallerName]
}

// ShardKey returns the shard key from the ShardKey transport header.
func (call *InboundCall) ShardKey() string {
	return call.headers[ShardKey]
}

// RoutingDelegate returns the routing delegate from the RoutingDelegate transport header.
func (call *InboundCall) RoutingDelegate() string {
	return call.headers[RoutingDelegate]
}

// RemotePeer returns the caller's peer info.
func (call *InboundCall) RemotePeer() PeerInfo {
	return call.conn.RemotePeerInfo()
}

// Reads the entire method name (arg1) from the request stream.
func (call *InboundCall) readMethod() error {
	var arg1 []byte
	if err := NewArgReader(call.arg1Reader()).Read(&arg1); err != nil {
		return call.failed(err)
	}

	call.method = arg1
	call.methodString = string(arg1)
	return nil
}

// Arg2Reader returns an ArgReader to read the second argument.
// The ReadCloser must be closed once the argument has been read.
func (call *InboundCall) Arg2Reader() (ArgReader, error) {
	return call.arg2Reader()
}

// Arg3Reader returns an ArgReader to read the last argument.
// The ReadCloser must be closed once the argument has been read.
func (call *InboundCall) Arg3Reader() (ArgReader, error) {
	return call.arg3Reader()
}

// Response provides access to the InboundCallResponse object which can be used
// to write back to the calling peer
func (call *InboundCall) Response() *InboundCallResponse {
	if call.err != nil {
		// While reading Thrift, we cannot distinguish between malformed Thrift and other errors,
		// and so we may try to respond with a bad request. We should ensure that the response
		// is marked as failed if the request has failed so that we don't try to shutdown the exchange
		// a second time.
		call.response.err = call.err
	}
	return call.response
}

func (call *InboundCall) doneReading(unexpected error) {}

// An InboundCallResponse is used to send the response back to the calling peer
type InboundCallResponse struct {
	reqResWriter
	Annotations

	call   *InboundCall
	cancel context.CancelFunc
	// calledAt is the time the inbound call was routed to the application.
	calledAt         time.Time
	applicationError bool
	systemError      bool
	headers          transportHeaders
	span             Span
	statsReporter    StatsReporter
	commonStatsTags  map[string]string
}

// SendSystemError returns a system error response to the peer.  The call is considered
// complete after this method is called, and no further data can be written.
func (response *InboundCallResponse) SendSystemError(err error) error {
	if response.err != nil {
		return response.err
	}
	// Fail all future attempts to read fragments
	response.state = reqResWriterComplete
	response.systemError = true
	response.doneSending()
	response.call.releasePreviousFragment()
	return response.conn.SendSystemError(response.mex.msgID, CurrentSpan(response.mex.ctx), err)
}

// SetApplicationError marks the response as being an application error.  This method can
// only be called before any arguments have been sent to the calling peer.
func (response *InboundCallResponse) SetApplicationError() error {
	if response.state > reqResWriterPreArg2 {
		return response.failed(errReqResWriterStateMismatch{
			state:         response.state,
			expectedState: reqResWriterPreArg2,
		})
	}
	response.applicationError = true
	return nil
}

// Arg2Writer returns a WriteCloser that can be used to write the second argument.
// The returned writer must be closed once the write is complete.
func (response *InboundCallResponse) Arg2Writer() (ArgWriter, error) {
	if err := NewArgWriter(response.arg1Writer()).Write(nil); err != nil {
		return nil, err
	}
	return response.arg2Writer()
}

// Arg3Writer returns a WriteCloser that can be used to write the last argument.
// The returned writer must be closed once the write is complete.
func (response *InboundCallResponse) Arg3Writer() (ArgWriter, error) {
	return response.arg3Writer()
}

// doneSending shuts down the message exchange for this call.
// For incoming calls, the last message is sending the call response.
func (response *InboundCallResponse) doneSending() {
	// TODO(prashant): Move this to when the message is actually being sent.
	now := response.GetTime()
	response.AddAnnotationAt(AnnotationKeyServerSend, now)
	response.Report()

	latency := now.Sub(response.calledAt)
	response.statsReporter.RecordTimer("inbound.calls.latency", response.commonStatsTags, latency)

	if response.systemError {
		// TODO(prashant): Report the error code type as per metrics doc and enable.
		// response.statsReporter.IncCounter("inbound.calls.system-errors", response.commonStatsTags, 1)
	} else if response.applicationError {
		response.statsReporter.IncCounter("inbound.calls.app-errors", response.commonStatsTags, 1)
	} else {
		response.statsReporter.IncCounter("inbound.calls.success", response.commonStatsTags, 1)
	}

	// Cancel the context since the response is complete.
	response.cancel()

	// The message exchange is still open if there are no errors, call shutdown.
	if response.err == nil {
		response.mex.shutdown()
	}
}
