// Copyright (c) 2019 Uber Technologies, Inc.
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

package client

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"

	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

type replicatedSessionTestSuite struct {
	suite.Suite
	mockCtrl          *gomock.Controller
	replicatedSession *replicatedSession
}

func newSessionFnWithSession(s clientSession) newSessionFn {
	return func(Options) (clientSession, error) {
		return s, nil
	}
}

func newTopologyInitializer() topology.Initializer {
	return topology.NewStaticInitializer(topology.NewStaticOptions())
}

func optionsWithAsyncSessions(hasSync bool, asyncCount int) Options {
	topoInits := make([]topology.Initializer, 0, asyncCount)
	for i := 0; i < asyncCount; i++ {
		topoInits = append(topoInits, newTopologyInitializer())
	}
	options := NewAdminOptions().
		SetAsyncTopologyInitializers(topoInits)
	if asyncCount > 0 {
		workerPool, err := xsync.NewPooledWorkerPool(10,
			xsync.NewPooledWorkerPoolOptions())
		if err != nil {
			panic(err)
		}
		workerPool.Init()
		options = options.SetAsyncWriteWorkerPool(workerPool)
	}

	if hasSync {
		options = options.SetTopologyInitializer(newTopologyInitializer())
	}
	return options
}

func (s *replicatedSessionTestSuite) initReplicatedSession(opts Options, newSessionFunc newSessionFn) {
	topoInits := opts.AsyncTopologyInitializers()
	overrides := make([]environment.ClientOverrides, len(topoInits))
	session, err := newReplicatedSession(
		opts,
		NewOptionsForAsyncClusters(opts, topoInits, overrides),
		withNewSessionFn(newSessionFunc),
	)
	s.replicatedSession = session.(*replicatedSession)
	s.NoError(err)
}

func (s *replicatedSessionTestSuite) SetupTest() {
	s.mockCtrl = gomock.NewController(s.T())
}

func (s *replicatedSessionTestSuite) TearDownTest() {
	s.mockCtrl.Finish()
}

func TestReplicatedSessionTestSuite(t *testing.T) {
	suite.Run(t, new(replicatedSessionTestSuite))
}

var constructorCreatesSessionsTests = []struct {
	options       Options
	expectedCount int
}{
	{
		options:       optionsWithAsyncSessions(false, 0),
		expectedCount: 0,
	},
	{
		options:       optionsWithAsyncSessions(false, 2),
		expectedCount: 2,
	},
	{
		options:       optionsWithAsyncSessions(true, 3),
		expectedCount: 4,
	},
	{
		options:       optionsWithAsyncSessions(true, 0),
		expectedCount: 1,
	},
}

func (s *replicatedSessionTestSuite) TestConstructorCreatesSessions() {
	for _, tt := range constructorCreatesSessionsTests {
		count := 0
		var newSessionFunc = func(opts Options) (clientSession, error) {
			count = count + 1
			return NewMockclientSession(s.mockCtrl), nil
		}

		s.initReplicatedSession(tt.options, newSessionFunc)
		s.Equal(tt.expectedCount, count)
	}
}

func (s *replicatedSessionTestSuite) TestSetSession() {
	opts := optionsWithAsyncSessions(false, 0)
	session := NewMockclientSession(s.mockCtrl)
	newSessionFunc := newSessionFnWithSession(session)
	s.initReplicatedSession(opts, newSessionFunc)

	topoInit := newTopologyInitializer()
	sessionOpts := NewMockAdminOptions(s.mockCtrl)
	sessionOpts.EXPECT().TopologyInitializer().Return(topoInit)

	s.Nil(s.replicatedSession.session)
	err := s.replicatedSession.setSession(sessionOpts)
	s.NoError(err)
	s.Equal(session, s.replicatedSession.session)
}

func (s *replicatedSessionTestSuite) TestSetAsyncSessions() {
	opts := optionsWithAsyncSessions(false, 0)
	session := NewMockclientSession(s.mockCtrl)
	newSessionFunc := newSessionFnWithSession(session)

	s.initReplicatedSession(opts, newSessionFunc)

	sessionOpts := []Options{}
	for i := 0; i < 3; i++ {
		o := NewMockAdminOptions(s.mockCtrl)
		o.EXPECT().InstrumentOptions().AnyTimes().Return(instrument.NewOptions())
		o.EXPECT().SetInstrumentOptions(gomock.Any()).Return(o)
		sessionOpts = append(sessionOpts, o)
	}

	s.Len(s.replicatedSession.asyncSessions, 0)
	err := s.replicatedSession.setAsyncSessions(sessionOpts)
	s.NoError(err)
	s.Len(s.replicatedSession.asyncSessions, 3)
}

func (s *replicatedSessionTestSuite) TestReplicate() {
	asyncCount := 2
	namespace := ident.StringID("foo")
	id := ident.StringID("bar")
	now := time.Now()
	value := float64(123)
	unit := xtime.Nanosecond
	annotation := []byte{}

	var newSessionFunc = func(opts Options) (clientSession, error) {
		s := NewMockclientSession(s.mockCtrl)
		s.EXPECT().Write(namespace, id, now, value, unit, annotation).Return(nil)
		return s, nil
	}

	opts := optionsWithAsyncSessions(true, asyncCount)
	s.initReplicatedSession(opts, newSessionFunc)
	s.replicatedSession.outCh = make(chan error)

	err := s.replicatedSession.Write(namespace, id, now, value, unit, annotation)
	s.NoError(err)

	t := time.NewTimer(1 * time.Second) // Allow async expectations to occur before ending test
	for i := 0; i < asyncCount; i++ {
		select {
		case err := <-s.replicatedSession.outCh:
			s.NoError(err)
		case <-t.C:
			break
		}
	}
}

func (s *replicatedSessionTestSuite) TestOpenReplicatedSession() {
	var newSessionFunc = func(opts Options) (clientSession, error) {
		s := NewMockclientSession(s.mockCtrl)
		s.EXPECT().Open().Return(nil)
		return s, nil
	}

	opts := optionsWithAsyncSessions(true, 2)
	s.initReplicatedSession(opts, newSessionFunc)
	s.replicatedSession.Open()
}

func (s *replicatedSessionTestSuite) TestOpenReplicatedSessionSyncError() {
	sessions := []*MockclientSession{}
	var newSessionFunc = func(opts Options) (clientSession, error) {
		s := NewMockclientSession(s.mockCtrl)
		sessions = append(sessions, s)
		return s, nil
	}

	opts := optionsWithAsyncSessions(true, 2)
	s.initReplicatedSession(opts, newSessionFunc)

	// Early exit if sync session Open() returns an error
	sessions[0].EXPECT().Open().Return(errors.New("an error"))
	s.replicatedSession.Open()
}

func (s *replicatedSessionTestSuite) TestOpenReplicatedSessionAsyncError() {
	sessions := []*MockclientSession{}
	var newSessionFunc = func(opts Options) (clientSession, error) {
		s := NewMockclientSession(s.mockCtrl)
		sessions = append(sessions, s)
		return s, nil
	}

	opts := optionsWithAsyncSessions(true, 2)
	s.initReplicatedSession(opts, newSessionFunc)

	// No early exit if async session Open() returns an error
	sessions[0].EXPECT().Open().Return(nil)
	sessions[1].EXPECT().Open().Return(errors.New("an error"))
	sessions[2].EXPECT().Open().Return(nil)
	s.replicatedSession.Open()
}
