package client

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/suite"
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

func optionsWithAsyncSessions(hasSync bool, asyncCount int) MultiClusterOptions {
	topoInits := make([]topology.Initializer, 0, asyncCount)
	for i := 0; i < asyncCount; i++ {
		topoInits = append(topoInits, newTopologyInitializer())
	}

	options := NewAdminOptions().(Options)
	if hasSync {
		options = options.SetTopologyInitializer(newTopologyInitializer())
	}

	return NewMultiClusterOptions().
		SetOptions(options).
		SetAsyncTopologyInitializers(topoInits)
}

func (s *replicatedSessionTestSuite) initReplicatedSession(opts MultiClusterOptions, newSessionFunc newSessionFn) {
	session, err := newReplicatedSession(
		opts,
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
	options       MultiClusterOptions
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

// func TestReplicatedSession(t *testing.T) {
// 	ctrl := gomock.NewController(t)

// 	var newMockSession = func(_ Options) (clientSession, error) {
// 		return NewMockclientSession(ctrl), nil
// 	}

// 	opts := NewMockMultiClusterOptions(ctrl)
// 	newReplicatedSession(opts, withNewSessionFn(newMockSession))
// }
