package client

import (
	"testing"

	"github.com/golang/mock/gomock"
)

func newReplicatedSessionTestOptions() MultiClusterOptions {
	return NewAdminMultiClusterOptions()
}

func TestReplicatedSession(t *testing.T) {
	ctrl := gomock.NewController(t)

	var newMockSession = func(_ Options) (clientSession, error) {
		return NewMockclientSession(ctrl), nil
	}

	opts := NewMockMultiClusterOptions(ctrl)
	newReplicatedSession(opts, withNewSessionFn(newMockSession))
}
