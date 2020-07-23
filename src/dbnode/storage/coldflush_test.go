package storage

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/stretchr/testify/require"
)

func TestColdFlushManagerFlushDoneFlushError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		fakeErr            = errors.New("fake error while marking flush done")
		mockPersistManager = persist.NewMockManager(ctrl)
		mockFlushPersist   = persist.NewMockFlushPreparer(ctrl)
	)

	mockFlushPersist.EXPECT().DoneFlush().Return(fakeErr)
	mockPersistManager.EXPECT().StartFlushPersist().Return(mockFlushPersist, nil)

	testOpts := DefaultTestOptions().SetPersistManager(mockPersistManager)
	db := newMockdatabase(ctrl)
	db.EXPECT().Options().Return(testOpts).AnyTimes()
	db.EXPECT().OwnedNamespaces().Return(nil, nil)

	cfm := newColdFlushManager(db, mockPersistManager, testOpts).(*coldFlushManager)
	cfm.pm = mockPersistManager

	require.EqualError(t, fakeErr, cfm.coldFlush().Error())
}
